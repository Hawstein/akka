/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.actor.dungeon

import scala.annotation.tailrec
import akka.dispatch.{ Envelope, Mailbox }
import akka.dispatch.sysmsg._
import akka.event.Logging.Error
import akka.util.Unsafe
import akka.actor._
import akka.serialization.SerializationExtension

import scala.util.control.{ NoStackTrace, NonFatal }
import scala.util.control.Exception.Catcher
import akka.dispatch.MailboxType
import akka.dispatch.ProducesMessageQueue
import akka.serialization.SerializerWithStringManifest
import akka.dispatch.UnboundedMailbox
import akka.serialization.DisabledJavaSerializer

private[akka] trait Dispatch {
  this: ActorCell ⇒

  @volatile private var _mailboxDoNotCallMeDirectly: Mailbox = _ //This must be volatile since it isn't protected by the mailbox status

  // 初始化邮箱
  @inline final def mailbox: Mailbox = Unsafe.instance.getObjectVolatile(this, AbstractActorCell.mailboxOffset).asInstanceOf[Mailbox]

  // 通过 CAS 设置当前邮箱
  @tailrec final def swapMailbox(newMailbox: Mailbox): Mailbox = {
    val oldMailbox = mailbox
    if (!Unsafe.instance.compareAndSwapObject(this, AbstractActorCell.mailboxOffset, oldMailbox, newMailbox)) swapMailbox(newMailbox)
    else oldMailbox
  }

  final def hasMessages: Boolean = mailbox.hasMessages

  final def numberOfMessages: Int = mailbox.numberOfMessages

  final def isTerminated: Boolean = mailbox.isClosed

  /**
   * Initialize this cell, i.e. set up mailboxes and supervision. The UID must be
   * reasonably different from the previous UID of a possible actor with the same path,
   * which can be achieved by using ThreadLocalRandom.current.nextInt().
   *
   * 初始化 ActorCell, 设置 mailbox 以及监管者
   */
  final def init(sendSupervise: Boolean, mailboxType: MailboxType): this.type = {
    /*
     * Create the mailbox and enqueue the Create() message to ensure that
     * this is processed before anything else.
     *
     * 创建邮箱, 然后将 Create 消息放入邮箱, 用于创建 actor
     */
    val mbox = dispatcher.createMailbox(this, mailboxType)

    /*
     * The mailboxType was calculated taking into account what the MailboxType
     * has promised to produce. If that was more than the default, then we need
     * to reverify here because the dispatcher may well have screwed it up.
     */
    // we need to delay the failure to the point of actor creation so we can handle
    // it properly in the normal way
    val actorClass = props.actorClass
    val createMessage = mailboxType match {
      case _: ProducesMessageQueue[_] if system.mailboxes.hasRequiredType(actorClass) ⇒
        val req = system.mailboxes.getRequiredType(actorClass)
        if (req isInstance mbox.messageQueue) Create(None) // 没有异常的创建消息
        else {
          val gotType = if (mbox.messageQueue == null) "null" else mbox.messageQueue.getClass.getName
          // 还有邮箱类型不匹配异常的创建消息
          Create(Some(ActorInitializationException(
            self,
            s"Actor [$self] requires mailbox type [$req] got [$gotType]")))
        }
      case _ ⇒ Create(None)
    }

    swapMailbox(mbox)
    mailbox.setActor(this) // 将邮箱和当前 actorCell 关联起来, 逻辑上 actorCell 是邮箱的一个属性

    // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
    /**
     * 将创建消息放入系统队列
     * 默认实现: [[akka.dispatch.DefaultSystemMessageQueue.systemEnqueue]]
     */
    mailbox.systemEnqueue(self, createMessage)

    if (sendSupervise) {
      // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
      // 让该 actorCell 的 parent 监管它
      parent.sendSystemMessage(akka.dispatch.sysmsg.Supervise(self, async = false))
    }
    this
  }

  final def initWithFailure(failure: Throwable): this.type = {
    val mbox = dispatcher.createMailbox(this, new UnboundedMailbox)
    swapMailbox(mbox)
    mailbox.setActor(this)
    // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
    val createMessage = Create(Some(ActorInitializationException(self, "failure while creating ActorCell", failure)))
    mailbox.systemEnqueue(self, createMessage)
    this
  }

  /**
   * Start this cell, i.e. attach it to the dispatcher.
   * 将当前 actorCell attach 到 dispatcher 以启动 actor
   */
  def start(): this.type = {
    // This call is expected to start off the actor by scheduling its mailbox.
    dispatcher.attach(this)
    this
  }

  private def handleException: Catcher[Unit] = {
    case e: InterruptedException ⇒
      system.eventStream.publish(Error(e, self.path.toString, clazz(actor), "interrupted during message send"))
      Thread.currentThread.interrupt()
    case NonFatal(e) ⇒
      val message = e match {
        case n: NoStackTrace ⇒ "swallowing exception during message send: " + n.getMessage
        case _               ⇒ "swallowing exception during message send" // stack trace includes message
      }
      system.eventStream.publish(Error(e, self.path.toString, clazz(actor), message))
  }

  // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
  final def suspend(): Unit = try dispatcher.systemDispatch(this, Suspend()) catch handleException

  // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
  final def resume(causedByFailure: Throwable): Unit = try dispatcher.systemDispatch(this, Resume(causedByFailure)) catch handleException

  // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
  final def restart(cause: Throwable): Unit = try dispatcher.systemDispatch(this, Recreate(cause)) catch handleException

  // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
  final def stop(): Unit = try dispatcher.systemDispatch(this, Terminate()) catch handleException

  // 发送用户消息的默认实现
  def sendMessage(msg: Envelope): Unit =
    try {
      val msgToDispatch =
        if (system.settings.SerializeAllMessages) serializeAndDeserialize(msg)
        else msg

      /**
       * this 即为 receiver, 是调用 ! 的 ActorRef.
       * 具体实现:
       * [[akka.dispatch.Dispatcher.dispatch()]]
       * [[akka.dispatch.BalancingDispatcher.dispatch()]]
       */
      dispatcher.dispatch(this, msgToDispatch)
    } catch handleException

  private def serializeAndDeserialize(envelope: Envelope): Envelope = {

    val unwrappedMessage =
      (envelope.message match {
        case DeadLetter(wrapped, _, _) ⇒ wrapped
        case other                     ⇒ other
      }).asInstanceOf[AnyRef]

    unwrappedMessage match {
      case _: NoSerializationVerificationNeeded ⇒ envelope
      case msg ⇒
        val deserializedMsg = serializeAndDeserializePayload(msg)
        envelope.message match {
          case dl: DeadLetter ⇒ envelope.copy(message = dl.copy(message = deserializedMsg))
          case _              ⇒ envelope.copy(message = deserializedMsg)
        }
    }
  }

  private def serializeAndDeserializePayload(obj: AnyRef): AnyRef = {
    val s = SerializationExtension(system)
    val serializer = s.findSerializerFor(obj)
    if (serializer.isInstanceOf[DisabledJavaSerializer] && !s.shouldWarnAboutJavaSerializer(obj.getClass, serializer))
      obj // skip check for known "local" messages
    else {
      val bytes = serializer.toBinary(obj)
      serializer match {
        case ser2: SerializerWithStringManifest ⇒
          val manifest = ser2.manifest(obj)
          s.deserialize(bytes, serializer.identifier, manifest).get
        case _ ⇒
          s.deserialize(bytes, obj.getClass).get
      }
    }
  }

  // 发送系统消息的默认实现
  override def sendSystemMessage(message: SystemMessage): Unit = try dispatcher.systemDispatch(this, message) catch handleException

}
