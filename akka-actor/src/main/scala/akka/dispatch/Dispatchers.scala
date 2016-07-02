/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.dispatch

import java.util.concurrent.{ ConcurrentHashMap, ThreadFactory }
import com.typesafe.config.{ ConfigFactory, Config }
import akka.actor.{ Scheduler, DynamicAccess, ActorSystem }
import akka.event.Logging.Warning
import akka.event.EventStream
import akka.ConfigurationException
import akka.util.Helpers.ConfigOps
import scala.concurrent.ExecutionContext

// Done by Hawstein
/**
 * DispatcherPrerequisites represents useful contextual pieces when constructing a MessageDispatcher
 * 构建 [[MessageDispatcher]] 时所需要的上下文信息, 默认实现是 [[DefaultDispatcherPrerequisites]]
 */
trait DispatcherPrerequisites {
  def threadFactory: ThreadFactory
  def eventStream: EventStream
  def scheduler: Scheduler
  def dynamicAccess: DynamicAccess
  def settings: ActorSystem.Settings
  def mailboxes: Mailboxes
  def defaultExecutionContext: Option[ExecutionContext]
}

/**
 * INTERNAL API
 *
 *
 */
private[akka] final case class DefaultDispatcherPrerequisites(
  val threadFactory:           ThreadFactory,
  val eventStream:             EventStream,
  val scheduler:               Scheduler,
  val dynamicAccess:           DynamicAccess,
  val settings:                ActorSystem.Settings,
  val mailboxes:               Mailboxes,
  val defaultExecutionContext: Option[ExecutionContext]) extends DispatcherPrerequisites

object Dispatchers {
  /**
   * The id of the default dispatcher, also the full key of the
   * configuration of the default dispatcher.
   *
   * 默认 dispatcher 的 id
   */
  final val DefaultDispatcherId = "akka.actor.default-dispatcher"
}

/**
 * Dispatchers are to be defined in configuration to allow for tuning
 * for different environments. Use the `lookup` method to create
 * a dispatcher as specified in configuration.
 *
 * Look in `akka.actor.default-dispatcher` section of the reference.conf
 * for documentation of dispatcher options.
 *
 * Dispatchers 用于从配置文件中读取 dispatcher 配置, 通过 lookup 方法来创建一个 dispatcher
 * dispatcher 配置可参考默认配置: akka.actor.default-dispatcher
 *
 * 唯一创建 Dispatchers 的地方在 [[akka.actor.ActorSystemImpl.dispatchers]], 即创建 ActorSystem 时从配置中创建 dispatchers
 */
class Dispatchers(val settings: ActorSystem.Settings, val prerequisites: DispatcherPrerequisites) {

  import Dispatchers._

  // 把 config wrap 到 CachingConfig 里加速 path 的查询(getPath) 和 string 的获取 getString
  val cachingConfig = new CachingConfig(settings.config)

  // 先从项目的配置文件中拿默认 dispatcher 的配置,
  // 如果没有, 再从 akka 项目中的默认配置中拿.
  val defaultDispatcherConfig: Config =
    idConfig(DefaultDispatcherId).withFallback(settings.config.getConfig(DefaultDispatcherId))

  /**
   * The one and only default dispatcher.
   *
   * 默认 dispatcher
   */
  def defaultGlobalDispatcher: MessageDispatcher = lookup(DefaultDispatcherId)

  /**
   * 用于存储 dispatcherId 到相应配置 Map
   * 通过 [[registerConfigurator]] 进入注册,
   * [[lookupConfigurator]] 查询时, 如果是新创建的 MessageDispatcherConfigurator, 也会写入该 map
   */
  private val dispatcherConfigurators = new ConcurrentHashMap[String, MessageDispatcherConfigurator]

  /**
   * Returns a dispatcher as specified in configuration. Please note that this
   * method _may_ create and return a NEW dispatcher, _every_ call.
   *
   * Throws ConfigurationException if the specified dispatcher cannot be found in the configuration.
   *
   * 返回配置中指定的一个 dispatcher, 每次调用会创建一个新的 dispatcher.
   * 因此一般调用它拿到一个指定 dispatcher 后可作为全局变量使用
   */
  def lookup(id: String): MessageDispatcher = lookupConfigurator(id).dispatcher()

  /**
   * Checks that the configuration provides a section for the given dispatcher.
   * This does not guarantee that no ConfigurationException will be thrown when
   * using this dispatcher, because the details can only be checked by trying
   * to instantiate it, which might be undesirable when just checking.
   *
   * 检查配置中是否有该 dispatcher
   */
  def hasDispatcher(id: String): Boolean = dispatcherConfigurators.containsKey(id) || cachingConfig.hasPath(id)

  // 在 ConcurrentHashMap 里用 id 查找 MessageDispatcherConfigurator, 有则返回；
  // 没有则查看是否有该 id 的配置, 有则创建, 没有则抛出异常
  private def lookupConfigurator(id: String): MessageDispatcherConfigurator = {
    dispatcherConfigurators.get(id) match {
      case null ⇒
        // It doesn't matter if we create a dispatcher configurator that isn't used due to concurrent lookup.
        // That shouldn't happen often and in case it does the actual ExecutorService isn't
        // created until used, i.e. cheap.
        val newConfigurator =
          if (cachingConfig.hasPath(id)) configuratorFrom(config(id))
          else throw new ConfigurationException(s"Dispatcher [$id] not configured")

        dispatcherConfigurators.putIfAbsent(id, newConfigurator) match {
          case null     ⇒ newConfigurator
          case existing ⇒ existing
        }

      case existing ⇒ existing
    }
  }

  /**
   * Register a [[MessageDispatcherConfigurator]] that will be
   * used by [[#lookup]] and [[#hasDispatcher]] instead of looking
   * up the configurator from the system configuration.
   * This enables dynamic addition of dispatchers, as used by the
   * [[akka.routing.BalancingPool]].
   *
   * A configurator for a certain id can only be registered once, i.e.
   * it can not be replaced. It is safe to call this method multiple times,
   * but only the first registration will be used. This method returns `true` if
   * the specified configurator was successfully registered.
   *
   * 注册 [[MessageDispatcherConfigurator]] 到 dispatcherConfigurators 便于后续快速查询
   * 每个 configurator 只有在第一次注册时才真正注册, 并且返回 null, 该函数返回 true.
   * [[akka.routing.BalancingPool]] 使用了该函数
   */
  def registerConfigurator(id: String, configurator: MessageDispatcherConfigurator): Boolean =
    dispatcherConfigurators.putIfAbsent(id, configurator) == null

  /**
   * INTERNAL API
   * 根据 id 拿 Config
   */
  private[akka] def config(id: String): Config = {
    config(id, settings.config.getConfig(id))
  }

  /**
   * INTERNAL API
   * 根据 id 拿到一个 Config, 并且提供了几个 fallback
   */
  private[akka] def config(id: String, appConfig: Config): Config = {
    import scala.collection.JavaConverters._
    def simpleName = id.substring(id.lastIndexOf('.') + 1)
    idConfig(id)
      .withFallback(appConfig)
      .withFallback(ConfigFactory.parseMap(Map("name" → simpleName).asJava))
      .withFallback(defaultDispatcherConfig)
  }

  // 根据 id 拿到一个 Config
  private def idConfig(id: String): Config = {
    import scala.collection.JavaConverters._
    ConfigFactory.parseMap(Map("id" → id).asJava)
  }

  /**
   * INTERNAL API
   *
   * Creates a dispatcher from a Config. Internal test purpose only.
   *
   * ex: from(config.getConfig(id))
   *
   * The Config must also contain a `id` property, which is the identifier of the dispatcher.
   *
   * Throws: IllegalArgumentException if the value of "type" is not valid
   *         IllegalArgumentException if it cannot create the MessageDispatcherConfigurator
   *
   * 给定 Config, 创建一个 dispatcher, 仅用于测试.
   */
  private[akka] def from(cfg: Config): MessageDispatcher = configuratorFrom(cfg).dispatcher()

  /**
   * INTERNAL API
   *
   * Creates a MessageDispatcherConfigurator from a Config.
   *
   * The Config must also contain a `id` property, which is the identifier of the dispatcher.
   *
   * Throws: IllegalArgumentException if the value of "type" is not valid
   *         IllegalArgumentException if it cannot create the MessageDispatcherConfigurator
   *
   * 读取配置并创建一个 MessageDispatcherConfigurator
   * 配置中必需指定 `id`
   */
  private def configuratorFrom(cfg: Config): MessageDispatcherConfigurator = {
    if (!cfg.hasPath("id")) throw new ConfigurationException("Missing dispatcher 'id' property in config: " + cfg.root.render)

    cfg.getString("type") match {
      case "Dispatcher" ⇒ new DispatcherConfigurator(cfg, prerequisites)
      case "BalancingDispatcher" ⇒
        // FIXME remove this case in 2.4
        throw new IllegalArgumentException("BalancingDispatcher is deprecated, use a BalancingPool instead. " +
          "During a migration period you can still use BalancingDispatcher by specifying the full class name: " +
          classOf[BalancingDispatcherConfigurator].getName)
      case "PinnedDispatcher" ⇒ new PinnedDispatcherConfigurator(cfg, prerequisites)
      case fqn ⇒
        val args = List(classOf[Config] → cfg, classOf[DispatcherPrerequisites] → prerequisites)
        prerequisites.dynamicAccess.createInstanceFor[MessageDispatcherConfigurator](fqn, args).recover({
          case exception ⇒
            throw new ConfigurationException(
              ("Cannot instantiate MessageDispatcherConfigurator type [%s], defined in [%s], " +
                "make sure it has constructor with [com.typesafe.config.Config] and " +
                "[akka.dispatch.DispatcherPrerequisites] parameters")
                .format(fqn, cfg.getString("id")), exception)
        }).get
    }
  }
}

/**
 * Configurator for creating [[akka.dispatch.Dispatcher]].
 * Returns the same dispatcher instance for for each invocation
 * of the `dispatcher()` method.
 *
 * 用于创建 [[Dispatcher]] 的配置
 * 唯一使用地方: 上面的 configuratorFrom 函数.
 * 使用得最多的 DispatcherConfigurator, ForkJoinPool 和 ThreadPool 用的都是这个
 */
class DispatcherConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
  extends MessageDispatcherConfigurator(config, prerequisites) {

  private val instance = new Dispatcher(
    this,
    config.getString("id"),
    config.getInt("throughput"),
    config.getNanosDuration("throughput-deadline-time"),
    configureExecutor(),
    config.getMillisDuration("shutdown-timeout"))

  /**
   * Returns the same dispatcher instance for each invocation
   */
  override def dispatcher(): MessageDispatcher = instance
}

/**
 * INTERNAL API
 */
private[akka] object BalancingDispatcherConfigurator {
  private val defaultRequirement =
    ConfigFactory.parseString("mailbox-requirement = akka.dispatch.MultipleConsumerSemantics")
  def amendConfig(config: Config): Config =
    if (config.getString("mailbox-requirement") != Mailboxes.NoMailboxRequirement) config
    else defaultRequirement.withFallback(config)
}

/**
 * Configurator for creating [[akka.dispatch.BalancingDispatcher]].
 * Returns the same dispatcher instance for for each invocation
 * of the `dispatcher()` method.
 */
class BalancingDispatcherConfigurator(_config: Config, _prerequisites: DispatcherPrerequisites)
  extends MessageDispatcherConfigurator(BalancingDispatcherConfigurator.amendConfig(_config), _prerequisites) {

  private val instance = {
    val mailboxes = prerequisites.mailboxes
    val id = config.getString("id")
    val requirement = mailboxes.getMailboxRequirement(config)
    if (!classOf[MultipleConsumerSemantics].isAssignableFrom(requirement))
      throw new IllegalArgumentException(
        "BalancingDispatcher must have 'mailbox-requirement' which implements akka.dispatch.MultipleConsumerSemantics; " +
          s"dispatcher [$id] has [$requirement]")
    val mailboxType =
      if (config.hasPath("mailbox")) {
        val mt = mailboxes.lookup(config.getString("mailbox"))
        if (!requirement.isAssignableFrom(mailboxes.getProducedMessageQueueType(mt)))
          throw new IllegalArgumentException(
            s"BalancingDispatcher [$id] has 'mailbox' [${mt.getClass}] which is incompatible with 'mailbox-requirement' [$requirement]")
        mt
      } else if (config.hasPath("mailbox-type")) {
        val mt = mailboxes.lookup(id)
        if (!requirement.isAssignableFrom(mailboxes.getProducedMessageQueueType(mt)))
          throw new IllegalArgumentException(
            s"BalancingDispatcher [$id] has 'mailbox-type' [${mt.getClass}] which is incompatible with 'mailbox-requirement' [$requirement]")
        mt
      } else mailboxes.lookupByQueueType(requirement)
    create(mailboxType)
  }

  protected def create(mailboxType: MailboxType): BalancingDispatcher =
    new BalancingDispatcher(
      this,
      config.getString("id"),
      config.getInt("throughput"),
      config.getNanosDuration("throughput-deadline-time"),
      mailboxType,
      configureExecutor(),
      config.getMillisDuration("shutdown-timeout"),
      config.getBoolean("attempt-teamwork"))

  /**
   * Returns the same dispatcher instance for each invocation
   */
  override def dispatcher(): MessageDispatcher = instance
}

/**
 * Configurator for creating [[akka.dispatch.PinnedDispatcher]].
 * Returns new dispatcher instance for for each invocation
 * of the `dispatcher()` method.
 */
class PinnedDispatcherConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
  extends MessageDispatcherConfigurator(config, prerequisites) {

  private val threadPoolConfig: ThreadPoolConfig = configureExecutor() match {
    case e: ThreadPoolExecutorConfigurator ⇒ e.threadPoolConfig
    case other ⇒
      prerequisites.eventStream.publish(
        Warning(
          "PinnedDispatcherConfigurator",
          this.getClass,
          "PinnedDispatcher [%s] not configured to use ThreadPoolExecutor, falling back to default config.".format(
            config.getString("id"))))
      ThreadPoolConfig()
  }
  /**
   * Creates new dispatcher for each invocation.
   */
  override def dispatcher(): MessageDispatcher =
    new PinnedDispatcher(
      this, null, config.getString("id"),
      config.getMillisDuration("shutdown-timeout"), threadPoolConfig)

}
