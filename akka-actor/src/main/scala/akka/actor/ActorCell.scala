/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.actor

import akka.actor.dungeon.ChildrenContainer
import akka.dispatch.Envelope
import akka.dispatch.sysmsg._
import akka.event.Logging.{ Debug, Error, LogEvent }
import akka.japi.Procedure
import java.io.{ NotSerializableException, ObjectOutputStream }

import scala.annotation.{ switch, tailrec }
import scala.collection.immutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration
import java.util.concurrent.ThreadLocalRandom

import scala.util.control.NonFatal
import akka.dispatch.MessageDispatcher
import akka.util.Reflect
import akka.japi.pf.ReceiveBuilder
import akka.actor.AbstractActor.Receive
import akka.annotation.InternalApi

// Done by Hawstein
/**
 * The actor context - the view of the actor cell from the actor.
 * Exposes contextual information for the actor and the current message.
 *
 * There are several possibilities for creating actors (see [[akka.actor.Props]]
 * for details on `props`):
 *
 * {{{
 * // Java or Scala
 * context.actorOf(props, "name")
 * context.actorOf(props)
 *
 * // Scala
 * context.actorOf(Props[MyActor])
 * context.actorOf(Props(classOf[MyActor], arg1, arg2), "name")
 *
 * // Java
 * getContext().actorOf(Props.create(MyActor.class));
 * getContext().actorOf(Props.create(MyActor.class, arg1, arg2), "name");
 * }}}
 *
 * Where no name is given explicitly, one will be automatically generated.
 *
 * ActorContext 用于存储 actor 的上下文信息, 其中定义的方法的具体实现在 [[ActorCell]] 及 [[akka.actor.dungeon]] 包中
 */
trait ActorContext extends ActorRefFactory {

  def self: ActorRef

  /**
   * Retrieve the Props which were used to create this actor.
   */
  def props: Props

  /**
   * Gets the current receive timeout.
   * When specified, the receive method should be able to handle a [[akka.actor.ReceiveTimeout]] message.
   */
  def receiveTimeout: Duration

  /**
   * Defines the inactivity timeout after which the sending of a [[akka.actor.ReceiveTimeout]] message is triggered.
   * When specified, the receive function should be able to handle a [[akka.actor.ReceiveTimeout]] message.
   * 1 millisecond is the minimum supported timeout.
   *
   * Please note that the receive timeout might fire and enqueue the `ReceiveTimeout` message right after
   * another message was enqueued; hence it is '''not guaranteed''' that upon reception of the receive
   * timeout there must have been an idle period beforehand as configured via this method.
   *
   * Once set, the receive timeout stays in effect (i.e. continues firing repeatedly after inactivity
   * periods). Pass in `Duration.Undefined` to switch off this feature.
   *
   * Messages marked with [[NotInfluenceReceiveTimeout]] will not reset the timer. This can be useful when
   * `ReceiveTimeout` should be fired by external inactivity but not influenced by internal activity,
   * e.g. scheduled tick messages.
   *
   * 设置 receiveTimeout, 如果超过这个时间没有接收到任何消息, 则会触发一个 ReceiveTimeout 消息, 该 actor 要去处理这个消息.
   * 消息如果 mix in 了 NotInfluenceReceiveTimeout 这个 trait, 则收到这类消息不会去重置 timer, 一般可用于内部那些不影响 timer 的消息,
   * 比如说周期性的 tick 信息
   */
  def setReceiveTimeout(timeout: Duration): Unit

  /**
   * Changes the Actor's behavior to become the new 'Receive' (PartialFunction[Any, Unit]) handler.
   * Replaces the current behavior on the top of the behavior stack.
   */
  def become(behavior: Actor.Receive): Unit = become(behavior, discardOld = true)

  /**
   * Changes the Actor's behavior to become the new 'Receive' (PartialFunction[Any, Unit]) handler.
   * This method acts upon the behavior stack as follows:
   *
   *  - if `discardOld = true` it will replace the top element (i.e. the current behavior)
   *  - if `discardOld = false` it will keep the current behavior and push the given one atop
   *
   * The default of replacing the current behavior on the stack has been chosen to avoid memory
   * leaks in case client code is written without consulting this documentation first (i.e.
   * always pushing new behaviors and never issuing an `unbecome()`)
   *
   * 从当前行为切换到新的行为, discard 为 true 会用新的行为替换行为栈上的顶部元素, 为 false 则直接压入行为栈顶部
   * discard 默认为 true, 为的是避免用户使用不当(没有使用 unbecome), 造成内在内存泄漏
   * 默认实现 [[ActorCell.become]]
   */
  def become(behavior: Actor.Receive, discardOld: Boolean): Unit

  /**
   * Reverts the Actor behavior to the previous one on the behavior stack.
   */
  def unbecome(): Unit

  /**
   * Returns the sender 'ActorRef' of the current message.
   *
   * 当前消息的发送者, 是一个 ActorRef
   */
  def sender(): ActorRef

  /**
   * Returns all supervised children; this method returns a view (i.e. a lazy
   * collection) onto the internal collection of children. Targeted lookups
   * should be using `child` instead for performance reasons:
   *
   * {{{
   * val badLookup = context.children find (_.path.name == "kid")
   * // should better be expressed as:
   * val goodLookup = context.child("kid")
   * }}}
   *
   * 返回所有受监管的子 actor, 想取具体的子 actor 推荐使用 context.child("kid"), 这样更快
   */
  def children: immutable.Iterable[ActorRef]

  /**
   * Get the child with the given name if it exists.
   */
  def child(name: String): Option[ActorRef]

  /**
   * Returns the dispatcher (MessageDispatcher) that is used for this Actor.
   * Importing this member will place an implicit ExecutionContext in scope.
   */
  implicit def dispatcher: ExecutionContextExecutor

  /**
   * The system that the actor belongs to.
   * Importing this member will place an implicit ActorSystem in scope.
   *
   * actor 所属的 ActorSystem
   */
  implicit def system: ActorSystem

  /**
   * Returns the supervising parent ActorRef.
   *
   * 返回监管者, 是一个 ActorRef
   */
  def parent: ActorRef

  /**
   * Registers this actor as a Monitor for the provided ActorRef.
   * This actor will receive a Terminated(subject) message when watched
   * actor is terminated.
   * @return the provided ActorRef
   *
   * 用该方法去监控其它 actor, 不需要是子 actor. 当被监控的 actor 终止时, 监控者会收到一个 Terminated(subject) 信息
   */
  def watch(subject: ActorRef): ActorRef

  /**
   * Unregisters this actor as Monitor for the provided ActorRef.
   * @return the provided ActorRef
   */
  def unwatch(subject: ActorRef): ActorRef

  /**
   * ActorContexts shouldn't be Serializable
   *
   * ActorContexts 不可序列化, 避免对外传递
   */
  final protected def writeObject(o: ObjectOutputStream): Unit =
    throw new NotSerializableException("ActorContext is not serializable!")
}

/**
 * UntypedActorContext is the UntypedActor equivalent of ActorContext,
 * containing the Java API
 */
@deprecated("Use AbstractActor.ActorContext instead of UntypedActorContext.", since = "2.5.0")
trait UntypedActorContext extends ActorContext {

  /**
   * Returns an unmodifiable Java Collection containing the linked actors,
   * please note that the backing map is thread-safe but not immutable
   */
  def getChildren(): java.lang.Iterable[ActorRef]

  /**
   * Returns a reference to the named child or null if no child with
   * that name exists.
   */
  def getChild(name: String): ActorRef

  /**
   * Changes the Actor's behavior to become the new 'Procedure' handler.
   * Replaces the current behavior on the top of the behavior stack.
   */
  def become(behavior: Procedure[Any]): Unit

  /**
   * Changes the Actor's behavior to become the new 'Procedure' handler.
   * This method acts upon the behavior stack as follows:
   *
   *  - if `discardOld = true` it will replace the top element (i.e. the current behavior)
   *  - if `discardOld = false` it will keep the current behavior and push the given one atop
   *
   * The default of replacing the current behavior on the stack has been chosen to avoid memory
   * leaks in case client code is written without consulting this documentation first (i.e.
   * always pushing new behaviors and never issuing an `unbecome()`)
   */
  def become(behavior: Procedure[Any], discardOld: Boolean): Unit

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait Cell {
  /**
   * The “self” reference which this Cell is attached to.
   */
  def self: ActorRef
  /**
   * The system within which this Cell lives.
   */
  def system: ActorSystem
  /**
   * The system internals where this Cell lives.
   */
  def systemImpl: ActorSystemImpl
  /**
   * Start the cell: enqueued message must not be processed before this has
   * been called. The usual action is to attach the mailbox to a dispatcher.
   */
  def start(): this.type
  /**
   * Recursively suspend this actor and all its children. Is only allowed to throw Fatal Throwables.
   */
  def suspend(): Unit
  /**
   * Recursively resume this actor and all its children. Is only allowed to throw Fatal Throwables.
   */
  def resume(causedByFailure: Throwable): Unit
  /**
   * Restart this actor (will recursively restart or stop all children). Is only allowed to throw Fatal Throwables.
   */
  def restart(cause: Throwable): Unit
  /**
   * Recursively terminate this actor and all its children. Is only allowed to throw Fatal Throwables.
   */
  def stop(): Unit
  /**
   * Returns “true” if the actor is locally known to be terminated, “false” if
   * alive or uncertain.
   */
  private[akka] def isTerminated: Boolean
  /**
   * The supervisor of this actor.
   *
   * [[ActorCell.parent]]
   */
  def parent: InternalActorRef
  /**
   * All children of this actor, including only reserved-names.
   */
  def childrenRefs: ChildrenContainer
  /**
   * Get the stats for the named child, if that exists.
   *
   * 默认实现: [[dungeon.Children.getChildByName]]
   */
  def getChildByName(name: String): Option[ChildStats]

  /**
   * Method for looking up a single child beneath this actor.
   * It is racy if called from the outside.
   *
   * 默认实现: [[dungeon.Children.getSingleChild]]
   */
  def getSingleChild(name: String): InternalActorRef

  /**
   * Enqueue a message to be sent to the actor; may or may not actually
   * schedule the actor to run, depending on which type of cell it is.
   * Is only allowed to throw Fatal Throwables.
   *
   * 默认实现: [[dungeon.Dispatch.sendMessage]]
   */
  def sendMessage(msg: Envelope): Unit

  /**
   * Enqueue a message to be sent to the actor; may or may not actually
   * schedule the actor to run, depending on which type of cell it is.
   * Is only allowed to throw Fatal Throwables.
   */
  final def sendMessage(message: Any, sender: ActorRef): Unit =
    sendMessage(Envelope(message, sender, system))

  /**
   * Enqueue a message to be sent to the actor; may or may not actually
   * schedule the actor to run, depending on which type of cell it is.
   * Is only allowed to throw Fatal Throwables.
   */
  def sendSystemMessage(msg: SystemMessage): Unit
  /**
   * Returns true if the actor is local, i.e. if it is actually scheduled
   * on a Thread in the current JVM when run.
   *
   * 默认实现: [[ActorCell.isLocal]]
   */
  def isLocal: Boolean
  /**
   * If the actor isLocal, returns whether "user messages" are currently queued,
   * “false” otherwise.
   *
   * 默认实现: [[dungeon.Dispatch.hasMessages]]
   */
  def hasMessages: Boolean
  /**
   * If the actor isLocal, returns the number of "user messages" currently queued,
   * which may be a costly operation, 0 otherwise.
   *
   * 默认实现: [[dungeon.Dispatch.numberOfMessages]]
   */
  def numberOfMessages: Int
  /**
   * The props for this actor cell.
   */
  def props: Props
}

/**
 * Everything in here is completely Akka PRIVATE. You will not find any
 * supported APIs in this place. This is not the API you were looking
 * for! (waves hand)
 */
private[akka] object ActorCell {
  // ThreadLocal 表示每个线程中有属于自己的 List[ActorContext] 互不影响, 是线程安全的
  val contextStack = new ThreadLocal[List[ActorContext]] {
    override def initialValue: List[ActorContext] = Nil
  }

  final val emptyCancellable: Cancellable = new Cancellable {
    def isCancelled: Boolean = false
    def cancel(): Boolean = false
  }

  final val emptyBehaviorStack: List[Actor.Receive] = Nil

  final val emptyActorRefSet: Set[ActorRef] = immutable.HashSet.empty

  final val terminatedProps: Props = Props((throw IllegalActorStateException("This Actor has been terminated")): Actor)

  // 用 0 表示未定义的 uid
  final val undefinedUid = 0

  @tailrec final def newUid(): Int = {
    // Note that this uid is also used as hashCode in ActorRef, so be careful
    // to not break hashing if you change the way uid is generated
    val uid = ThreadLocalRandom.current.nextInt()
    if (uid == undefinedUid) newUid
    else uid
  }

  final def splitNameAndUid(name: String): (String, Int) = {
    val i = name.indexOf('#')
    if (i < 0) (name, undefinedUid)
    else (name.substring(0, i), Integer.valueOf(name.substring(i + 1)))
  }

  final val DefaultState = 0
  final val SuspendedState = 1
  final val SuspendedWaitForChildrenState = 2
}

//ACTORCELL is NOT 64 bytes aligned, unless it is demonstrated to have a large improvement on performance
//vars don't need volatile since it's protected with the mailbox status
//Make sure that they are not read/written outside of a message processing (systemInvoke/invoke)
/**
 * Everything in here is completely Akka PRIVATE. You will not find any
 * supported APIs in this place. This is not the API you were looking
 * for! (waves hand)
 *
 *
 */
private[akka] class ActorCell(
  val system:      ActorSystemImpl,
  val self:        InternalActorRef,
  final val props: Props, // Must be final so that it can be properly cleared in clearActorCellFields
  val dispatcher:  MessageDispatcher,
  val parent:      InternalActorRef)
  extends UntypedActorContext with AbstractActor.ActorContext with Cell
  with dungeon.ReceiveTimeout
  with dungeon.Children
  with dungeon.Dispatch
  with dungeon.DeathWatch
  with dungeon.FaultHandling {

  import ActorCell._

  // ActorCell 用于 local actor
  final def isLocal = true

  final def systemImpl = system
  protected final def guardian = self
  protected final def lookupRoot = self
  final def provider = system.provider

  protected def uid: Int = self.path.uid
  private[this] var _actor: Actor = _
  def actor: Actor = _actor
  protected def actor_=(a: Actor): Unit = _actor = a
  // 当前处理的消息
  var currentMessage: Envelope = _
  // 行为栈, 使用 List 存储
  private var behaviorStack: List[Actor.Receive] = emptyBehaviorStack
  private[this] var sysmsgStash: LatestFirstSystemMessageList = SystemMessageList.LNil

  // Java API
  final def getParent() = parent
  // Java API
  final def getSystem() = system

  // 暂存系统消息
  protected def stash(msg: SystemMessage): Unit = {
    assert(msg.unlinked)
    sysmsgStash ::= msg
  }

  // 取出所有暂存的系统消息
  private def unstashAll(): LatestFirstSystemMessageList = {
    val unstashed = sysmsgStash
    sysmsgStash = SystemMessageList.LNil
    unstashed
  }

  /*
   * MESSAGE PROCESSING
   */
  //Memory consistency is handled by the Mailbox (reading mailbox status then processing messages, then writing mailbox status
  // Mailbox 中处理系统消息时, 会调用 systemInvoke
  final def systemInvoke(message: SystemMessage): Unit = {
    /*
     * When recreate/suspend/resume are received while restarting (i.e. between
     * preRestart and postRestart, waiting for children to terminate), these
     * must not be executed immediately, but instead queued and released after
     * finishRecreate returns. This can only ever be triggered by
     * ChildTerminated, and ChildTerminated is not one of the queued message
     * types (hence the overwrite further down). Mailbox sets message.next=null
     * before systemInvoke, so this will only be non-null during such a replay.
     */

    def calculateState: Int =
      if (waitingForChildrenOrNull ne null) SuspendedWaitForChildrenState
      else if (mailbox.isSuspended) SuspendedState
      else DefaultState

    @tailrec def sendAllToDeadLetters(messages: EarliestFirstSystemMessageList): Unit =
      if (messages.nonEmpty) {
        val tail = messages.tail
        val msg = messages.head
        msg.unlink()
        provider.deadLetters ! msg
        sendAllToDeadLetters(tail)
      }

    def shouldStash(m: SystemMessage, state: Int): Boolean =
      (state: @switch) match {
        case DefaultState                  ⇒ false
        case SuspendedState                ⇒ m.isInstanceOf[StashWhenFailed]
        case SuspendedWaitForChildrenState ⇒ m.isInstanceOf[StashWhenWaitingForChildren]
      }

    @tailrec
    def invokeAll(messages: EarliestFirstSystemMessageList, currentState: Int): Unit = {
      val rest = messages.tail
      val message = messages.head
      message.unlink()
      try {
        // 要处理的系统消息, 统一在这里处理
        message match {
          case message: SystemMessage if shouldStash(message, currentState) ⇒ stash(message)
          case f: Failed ⇒ handleFailure(f)
          case DeathWatchNotification(a, ec, at) ⇒ watchedActorTerminated(a, ec, at)
          case Create(failure) ⇒ create(failure) // 创建 actor, 创建完邮箱后放入的第一条消息
          case Watch(watchee, watcher) ⇒ addWatcher(watchee, watcher)
          case Unwatch(watchee, watcher) ⇒ remWatcher(watchee, watcher)
          case Recreate(cause) ⇒ faultRecreate(cause)
          case Suspend() ⇒ faultSuspend()
          case Resume(inRespToFailure) ⇒ faultResume(inRespToFailure)
          case Terminate() ⇒ terminate()
          case Supervise(child, async) ⇒ supervise(child, async)
          case NoMessage ⇒ // only here to suppress warning
        }
      } catch handleNonFatalOrInterruptedException { e ⇒
        handleInvokeFailure(Nil, e)
      }
      val newState = calculateState
      // As each state accepts a strict subset of another state, it is enough to unstash if we "walk up" the state
      // chain
      val todo = if (newState < currentState) unstashAll() reverse_::: rest else rest

      if (isTerminated) sendAllToDeadLetters(todo)
      else if (todo.nonEmpty) invokeAll(todo, newState)
    }

    invokeAll(new EarliestFirstSystemMessageList(message), calculateState)
  }

  //Memory consistency is handled by the Mailbox (reading mailbox status then processing messages, then writing mailbox status
  // Mailbox 中处理消息时, 会调用 invoke
  final def invoke(messageHandle: Envelope): Unit = {
    // 是否会影响 actor 中设置的 receiveTimeout
    val influenceReceiveTimeout = !messageHandle.message.isInstanceOf[NotInfluenceReceiveTimeout]
    try {
      currentMessage = messageHandle
      if (influenceReceiveTimeout) // 如果会影响 actor 中的 receiveTimeout 设置, 则先取消它(因为收到消息了), 下面会重置
        cancelReceiveTimeout()
      messageHandle.message match {
        case msg: AutoReceivedMessage ⇒ autoReceiveMessage(messageHandle) // 处理预定义的消息
        case msg                      ⇒ receiveMessage(msg)
      }
      // 处理完后设置当前消息为 null
      currentMessage = null // reset current message after successful invocation
    } catch handleNonFatalOrInterruptedException { e ⇒
      handleInvokeFailure(Nil, e)
    } finally {
      if (influenceReceiveTimeout)
        checkReceiveTimeout // Reschedule receive timeout 重新设置 receiveTimeout
    }
  }

  // 处理预定义的一些消息, 如 Terminated/Kill/PoisonPill 等
  def autoReceiveMessage(msg: Envelope): Unit = {
    if (system.settings.DebugAutoReceive)
      publish(Debug(self.path.toString, clazz(actor), "received AutoReceiveMessage " + msg))

    msg.message match {
      case t: Terminated              ⇒ receivedTerminated(t)
      case AddressTerminated(address) ⇒ addressTerminated(address)
      case Kill                       ⇒ throw ActorKilledException("Kill")
      case PoisonPill                 ⇒ self.stop()
      case sel: ActorSelectionMessage ⇒ receiveSelection(sel)
      case Identify(messageId)        ⇒ sender() ! ActorIdentity(messageId, Some(self))
    }
  }

  private def receiveSelection(sel: ActorSelectionMessage): Unit =
    if (sel.elements.isEmpty)
      invoke(Envelope(sel.msg, sender(), system))
    else
      ActorSelection.deliverSelection(self, sender(), sel)

  // 使用用户定义的行为来处理消息
  // 用户可以定义多个行为, 放在一个 behaviorStack 中(是一个 List[Receive]),
  // 使用头部行为处理当前消息
  final def receiveMessage(msg: Any): Unit = actor.aroundReceive(behaviorStack.head, msg)

  /*
   * ACTOR CONTEXT IMPLEMENTATION
   * ActorContext.sender() 的实现, 当前消息的发送者
   */

  final def sender(): ActorRef = currentMessage match {
    case null                      ⇒ system.deadLetters
    case msg if msg.sender ne null ⇒ msg.sender
    case _                         ⇒ system.deadLetters
  }

  def become(behavior: Actor.Receive, discardOld: Boolean = true): Unit =
    behaviorStack = behavior :: (if (discardOld && behaviorStack.nonEmpty) behaviorStack.tail else behaviorStack)

  // Java API
  def become(behavior: Procedure[Any]): Unit = become(behavior, discardOld = true)

  // Java API
  def become(behavior: Procedure[Any], discardOld: Boolean): Unit =
    become({ case msg ⇒ behavior.apply(msg) }: Actor.Receive, discardOld)

  def unbecome(): Unit = {
    val original = behaviorStack
    behaviorStack =
      if (original.isEmpty || original.tail.isEmpty) actor.receive :: emptyBehaviorStack
      else original.tail
  }

  /*
   * ACTOR INSTANCE HANDLING
   */

  //This method is in charge of setting up the contextStack and create a new instance of the Actor
  // 创建一个新的 Actor 实例, 并加多一个 ActorContext 到 contextStack 中
  // 这里把 this (ActorCell) 加到 contextStack 中, 注意 ActorCell 也是一个 ActorContext
  protected def newActor(): Actor = {
    contextStack.set(this :: contextStack.get)
    try {
      behaviorStack = emptyBehaviorStack
      // 创建一个新的 actor 实例
      val instance = props.newActor()

      if (instance eq null)
        throw ActorInitializationException(self, "Actor instance passed to actorOf can't be 'null'")

      // If no becomes were issued, the actors behavior is its receive method
      // 如果没有触发 becomes, actor 的当前行为即它的 receive 方法
      // !!! 非常关键的一步, 创建完 Actor 后把它默认的行为 receive 放到行为栈中
      behaviorStack = if (behaviorStack.isEmpty) instance.receive :: behaviorStack else behaviorStack
      instance
    } finally {
      val stackAfter = contextStack.get
      // 将 null 标志及当前 context 从 contextStack 中去掉
      if (stackAfter.nonEmpty)
        contextStack.set(if (stackAfter.head eq null) stackAfter.tail.tail else stackAfter.tail) // pop null marker plus our context
    }
  }

  // 创建 actor 并调用 preStart 方法
  protected def create(failure: Option[ActorInitializationException]): Unit = {
    def clearOutActorIfNonNull(): Unit = {
      if (actor != null) {
        clearActorFields(actor, recreate = false)
        actor = null // ensure that we know that we failed during creation
      }
    }

    failure foreach { throw _ }

    try {
      val created = newActor()
      actor = created
      // 调用 actor 的 preStart方法
      created.aroundPreStart()
      checkReceiveTimeout
      if (system.settings.DebugLifecycle) publish(Debug(self.path.toString, clazz(created), "started (" + created + ")"))
    } catch {
      case e: InterruptedException ⇒
        clearOutActorIfNonNull()
        Thread.currentThread().interrupt()
        throw ActorInitializationException(self, "interruption during creation", e)
      case NonFatal(e) ⇒
        clearOutActorIfNonNull()
        e match {
          case i: InstantiationException ⇒ throw ActorInitializationException(
            self,
            """exception during creation, this problem is likely to occur because the class of the Actor you tried to create is either,
               a non-static inner class (in which case make it a static inner class or use Props(new ...) or Props( new Creator ... )
               or is missing an appropriate, reachable no-args constructor.
              """, i.getCause)
          case x ⇒ throw ActorInitializationException(self, "exception during creation", x)
        }
    }
  }

  // 监管子 actor, 这是子 actor 创建后做的第一件事
  private def supervise(child: ActorRef, async: Boolean): Unit =
    if (!isTerminating) {
      // Supervise is the first thing we get from a new child, so store away the UID for later use in handleFailure()
      initChild(child) match {
        case Some(crs) ⇒
          handleSupervise(child, async)
          if (system.settings.DebugLifecycle) publish(Debug(self.path.toString, clazz(actor), "now supervising " + child))
        case None ⇒ publish(Error(self.path.toString, clazz(actor), "received Supervise from unregistered child " + child + ", this will not end well"))
      }
    }

  // future extension point
  protected def handleSupervise(child: ActorRef, async: Boolean): Unit = child match {
    case r: RepointableActorRef if async ⇒ r.point(catchFailures = true)
    case _                               ⇒
  }

  final protected def clearActorCellFields(cell: ActorCell): Unit = {
    cell.unstashAll()
    if (!Reflect.lookupAndSetField(classOf[ActorCell], cell, "props", ActorCell.terminatedProps))
      throw new IllegalArgumentException("ActorCell has no props field")
  }

  final protected def clearActorFields(actorInstance: Actor, recreate: Boolean): Unit = {
    setActorFields(actorInstance, context = null, self = if (recreate) self else system.deadLetters)
    currentMessage = null
    behaviorStack = emptyBehaviorStack
  }

  final protected def setActorFields(actorInstance: Actor, context: ActorContext, self: ActorRef): Unit =
    if (actorInstance ne null) {
      if (!Reflect.lookupAndSetField(actorInstance.getClass, actorInstance, "context", context)
        || !Reflect.lookupAndSetField(actorInstance.getClass, actorInstance, "self", self))
        throw IllegalActorStateException(actorInstance.getClass + " is not an Actor since it have not mixed in the 'Actor' trait")
    }

  // logging is not the main purpose, and if it fails there’s nothing we can do
  // 把 logEvent 发到 eventStream
  protected final def publish(e: LogEvent): Unit = try system.eventStream.publish(e) catch { case NonFatal(_) ⇒ }

  protected final def clazz(o: AnyRef): Class[_] = if (o eq null) this.getClass else o.getClass
}
