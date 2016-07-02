/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.dispatch

import akka.event.Logging.Error
import akka.actor.ActorCell
import akka.event.Logging
import akka.dispatch.sysmsg.SystemMessage
import java.util.concurrent.{ ExecutorService, RejectedExecutionException }
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

// Done by Hawstein
/**
 * The event-based ``Dispatcher`` binds a set of Actors to a thread pool backed up by a
 * `BlockingQueue`.
 *
 * The preferred way of creating dispatchers is to define configuration of it and use the
 * the `lookup` method in [[akka.dispatch.Dispatchers]].
 *
 * @param throughput positive integer indicates the dispatcher will only process so much messages at a time from the
 *                   mailbox, without checking the mailboxes of other actors. Zero or negative means the dispatcher
 *                   always continues until the mailbox is empty.
 *                   Larger values (or zero or negative) increase throughput, smaller values increase fairness
 *
 * 基于事件的 Dispatcher 将一组 actors 绑定到一个线程池, 后面使用的是 BlockingQueue
 * 创建 dispatchers 的推荐作法是在配置中定义它, 然后使用 lookup 方法来读取
 * 使用参见: [[DispatcherConfigurator]]
 *
 * throughput: 该数值表示 dispatcher 一次性从 mailbox 中拿多少条信息进行处理.
 * 0 或负数表示 dispatcher 会一直取信息直到 mailbox 为空.
 * 增大该值会增加吞吐量, 减少该值会增加公平性(信息被拿到的概率趋向接近)
 * 使用了 throughput 的地方:
 * [[Mailbox.processMailbox]] (重点)
 * [[akka.io.SelectionHandler]]
 */
class Dispatcher(
  _configurator:                  MessageDispatcherConfigurator,
  val id:                         String,
  val throughput:                 Int,
  val throughputDeadlineTime:     Duration,
  executorServiceFactoryProvider: ExecutorServiceFactoryProvider,
  val shutdownTimeout:            FiniteDuration)
  extends MessageDispatcher(_configurator) {

  import configurator.prerequisites._

  private class LazyExecutorServiceDelegate(factory: ExecutorServiceFactory) extends ExecutorServiceDelegate {
    /**
     * 根据不同的实现, 拿到不同的 executor, 如:
     * [[ForkJoinExecutorConfigurator.ForkJoinExecutorServiceFactory.createExecutorService]]
     * [[ThreadPoolExecutorConfigurator.threadPoolConfig.ThreadPoolExecutorServiceFactory.createExecutorService]]
     */
    lazy val executor: ExecutorService = factory.createExecutorService
    def copy(): LazyExecutorServiceDelegate = new LazyExecutorServiceDelegate(factory)
  }

  /**
   * 使用 LazyExecutorServiceDelegate wrap ExecutorServiceFactory, 将任务 delegate 给相应的 excutor
   * createExecutorServiceFactory 的具体实现:
   * [[ForkJoinExecutorConfigurator.createExecutorServiceFactory()]]
   * [[ThreadPoolExecutorConfigurator.createExecutorServiceFactory()]]
   */
  @volatile private var executorServiceDelegate: LazyExecutorServiceDelegate =
    new LazyExecutorServiceDelegate(executorServiceFactoryProvider.createExecutorServiceFactory(id, threadFactory))

  protected final def executorService: ExecutorServiceDelegate = executorServiceDelegate

  /**
   * INTERNAL API
   *
   * 收到消息时, 将消息加入到接收者的 mailbox, 然后触发一次处理
   * dispatch 处理用户定义的消息
   * 使用处: [[akka.actor.dungeon.Dispatch.sendMessage()]]
   */
  protected[akka] def dispatch(receiver: ActorCell, invocation: Envelope): Unit = {
    val mbox = receiver.mailbox
    mbox.enqueue(receiver.self, invocation)
    registerForExecution(mbox, hasMessageHint = true, hasSystemMessageHint = false)
  }

  /**
   * INTERNAL API
   *
   * systemDispatch 处理系统消息
   */
  protected[akka] def systemDispatch(receiver: ActorCell, invocation: SystemMessage): Unit = {
    val mbox = receiver.mailbox
    mbox.systemEnqueue(receiver.self, invocation)
    registerForExecution(mbox, hasMessageHint = false, hasSystemMessageHint = true)
  }

  /**
   * INTERNAL API
   */
  protected[akka] def executeTask(invocation: TaskInvocation) {
    try {
      executorService execute invocation
    } catch {
      case e: RejectedExecutionException ⇒
        try {
          executorService execute invocation
        } catch {
          case e2: RejectedExecutionException ⇒
            eventStream.publish(Error(e, getClass.getName, getClass, "executeTask was rejected twice!"))
            throw e2
        }
    }
  }

  /**
   * INTERNAL API
   *
   * 创建 mailbox
   */
  protected[akka] def createMailbox(actor: akka.actor.Cell, mailboxType: MailboxType): Mailbox = {
    new Mailbox(mailboxType.create(Some(actor.self), Some(actor.system))) with DefaultSystemMessageQueue
  }

  private val esUpdater = AtomicReferenceFieldUpdater.newUpdater(
    classOf[Dispatcher],
    classOf[LazyExecutorServiceDelegate],
    "executorServiceDelegate")

  /**
   * INTERNAL API
   */
  protected[akka] def shutdown: Unit = {
    val newDelegate = executorServiceDelegate.copy() // Doesn't matter which one we copy
    val es = esUpdater.getAndSet(this, newDelegate)
    es.shutdown()
  }

  /**
   * Returns if it was registered
   *
   * INTERNAL API
   *
   * 处理 mailbox 中的信息, mailbox 实现了 Runnalbe 接口, 然后通过 ExecutorService 调用执行
   *
   * @param mbox 需要在线程池里处理的邮箱
   * @param hasMessageHint 是否是用户定义的消息
   * @param hasSystemMessageHint 是否是系统消息
   * @return
   */
  protected[akka] override def registerForExecution(mbox: Mailbox, hasMessageHint: Boolean, hasSystemMessageHint: Boolean): Boolean = {
    if (mbox.canBeScheduledForExecution(hasMessageHint, hasSystemMessageHint)) { //This needs to be here to ensure thread safety and no races
      if (mbox.setAsScheduled()) {
        try {
          /**
           * 向 executorService 提交任务, 具体实现:
           * [[akka.dispatch.ForkJoinExecutorConfigurator.AkkaForkJoinPool.execute()]]
           * [[java.util.concurrent.ThreadPoolExecutor.execute()]]
           */
          executorService execute mbox
          true
        } catch {
          case e: RejectedExecutionException ⇒
            try {
              // 重试一次
              executorService execute mbox
              true
            } catch { //Retry once
              case e: RejectedExecutionException ⇒
                mbox.setAsIdle()
                eventStream.publish(Error(e, getClass.getName, getClass, "registerForExecution was rejected twice!"))
                throw e
            }
        }
      } else false
    } else false
  }

  override val toString: String = Logging.simpleName(this) + "[" + id + "]"
}

// 暂无使用
object PriorityGenerator {
  /**
   * Creates a PriorityGenerator that uses the supplied function as priority generator
   */
  def apply(priorityFunction: Any ⇒ Int): PriorityGenerator = new PriorityGenerator {
    def gen(message: Any): Int = priorityFunction(message)
  }
}

// 暂无使用
/**
 * A PriorityGenerator is a convenience API to create a Comparator that orders the messages of a
 * PriorityDispatcher
 */
abstract class PriorityGenerator extends java.util.Comparator[Envelope] {
  def gen(message: Any): Int

  final def compare(thisMessage: Envelope, thatMessage: Envelope): Int =
    gen(thisMessage.message) - gen(thatMessage.message)
}
