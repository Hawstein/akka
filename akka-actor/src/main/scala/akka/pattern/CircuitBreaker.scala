/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.pattern

import java.util.Optional
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger, AtomicLong }
import java.util.function.Consumer

import akka.AkkaException
import akka.actor.Scheduler
import akka.util.Unsafe

import scala.util.control.NoStackTrace
import java.util.concurrent.{ Callable, CompletionStage, CopyOnWriteArrayList }
import java.util.function.BiFunction

import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.concurrent.TimeoutException
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext

import scala.compat.java8.FutureConverters

/**
 * Companion object providing factory methods for Circuit Breaker which runs callbacks in caller's thread
 *
 * 该伴生对象提供的工厂方法创建的断路器, 会在调用的线程中执行回调函数. 如果想使用另外的线程, 可以自己 new 对象.
 */
object CircuitBreaker {

  /**
   * Create a new CircuitBreaker.
   *
   * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
   * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
   * executor in the constructor.
   *
   * @param scheduler Reference to Akka scheduler
   * @param maxFailures Maximum number of failures before opening the circuit
   * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
   * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
   *
   * 创建一个断路器, 回调函数会在调用者的同一线程执行.
   */
  def apply(scheduler: Scheduler, maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration): CircuitBreaker =
    new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout)(sameThreadExecutionContext)

  /**
   * Java API: Create a new CircuitBreaker.
   *
   * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
   * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
   * executor in the constructor.
   *
   * @param scheduler Reference to Akka scheduler
   * @param maxFailures Maximum number of failures before opening the circuit
   * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
   * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
   */
  def create(scheduler: Scheduler, maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration): CircuitBreaker =
    apply(scheduler, maxFailures, callTimeout, resetTimeout)

  private val exceptionAsFailure: Try[_] ⇒ Boolean = {
    case _: Success[_] ⇒ false
    case _             ⇒ true
  }

  private def exceptionAsFailureJava[T]: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean] =
    new BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean] {
      override def apply(t: Optional[T], err: Optional[Throwable]) = {
        if (err.isPresent)
          true
        else
          false
      }
    }

  protected def convertJavaFailureFnToScala[T](javaFn: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean]): Try[T] ⇒ Boolean = {
    val failureFnInScala: Try[T] ⇒ Boolean = {
      case Success(t)   ⇒ javaFn(Optional.of(t), Optional.empty())
      case Failure(err) ⇒ javaFn(Optional.empty(), Optional.of(err))
    }
    failureFnInScala
  }
}

/**
 * Provides circuit breaker functionality to provide stability when working with "dangerous" operations, e.g. calls to
 * remote systems
 *
 * Transitions through three states:
 * - In *Closed* state, calls pass through until the `maxFailures` count is reached.  This causes the circuit breaker
 * to open.  Both exceptions and calls exceeding `callTimeout` are considered failures.
 * - In *Open* state, calls fail-fast with an exception.  After `resetTimeout`, circuit breaker transitions to
 * half-open state.
 * - In *Half-Open* state, the first call will be allowed through, if it succeeds the circuit breaker will reset to
 * closed state.  If it fails, the circuit breaker will re-open to open state.  All calls beyond the first that
 * execute while the first is running will fail-fast with an exception.
 *
 * @param scheduler Reference to Akka scheduler
 * @param maxFailures Maximum number of failures before opening the circuit
 * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
 * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
 * @param executor [[scala.concurrent.ExecutionContext]] used for execution of state transition listeners
 */
class CircuitBreaker(
  scheduler:                Scheduler,
  maxFailures:              Int,
  callTimeout:              FiniteDuration,
  resetTimeout:             FiniteDuration,
  maxResetTimeout:          FiniteDuration,
  exponentialBackoffFactor: Double)(implicit executor: ExecutionContext) extends AbstractCircuitBreaker {

  require(exponentialBackoffFactor >= 1.0, "factor must be >= 1.0")

  def this(executor: ExecutionContext, scheduler: Scheduler, maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration) = {
    this(scheduler, maxFailures, callTimeout, resetTimeout, 36500.days, 1.0)(executor)
  }

  // add the old constructor to make it binary compatible
  def this(scheduler: Scheduler, maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration)(implicit executor: ExecutionContext) = {
    this(scheduler, maxFailures, callTimeout, resetTimeout, 36500.days, 1.0)(executor)
  }

  /**
   * The `resetTimeout` will be increased exponentially for each failed attempt to close the circuit.
   * The default exponential backoff factor is 2.
   *
   * @param maxResetTimeout the upper bound of resetTimeout
   */
  def withExponentialBackoff(maxResetTimeout: FiniteDuration): CircuitBreaker = {
    new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout, maxResetTimeout, 2.0)(executor)
  }

  /**
   * Holds reference to current state of CircuitBreaker - *access only via helper methods*
   *
   * 该值用来保存当前状态, 需要设置为 var, 因为该值会由 [[swapState]] 进行修改
   */
  @volatile
  private[this] var _currentStateDoNotCallMeDirectly: State = Closed

  /**
   * Holds reference to current resetTimeout of CircuitBreaker - *access only via helper methods*
   */
  @volatile
  private[this] var _currentResetTimeoutDoNotCallMeDirectly: FiniteDuration = resetTimeout

  /**
   * Helper method for access to underlying state via Unsafe
   *
   * @param oldState Previous state on transition
   * @param newState Next state on transition
   * @return Whether the previous state matched correctly
   *
   * 通过 Unsafe 来修改当前状态, 线程安全
   */
  @inline
  private[this] def swapState(oldState: State, newState: State): Boolean =
    Unsafe.instance.compareAndSwapObject(this, AbstractCircuitBreaker.stateOffset, oldState, newState)

  /**
   * Helper method for accessing underlying state via Unsafe
   *
   * @return Reference to current state
   *
   * 通过 Unsafe 来拿到当前的状态
   */
  @inline
  private[this] def currentState: State =
    Unsafe.instance.getObjectVolatile(this, AbstractCircuitBreaker.stateOffset).asInstanceOf[State]

  /**
   * Helper method for updating the underlying resetTimeout via Unsafe
   */
  @inline
  private[this] def swapResetTimeout(oldResetTimeout: FiniteDuration, newResetTimeout: FiniteDuration): Boolean =
    Unsafe.instance.compareAndSwapObject(this, AbstractCircuitBreaker.resetTimeoutOffset, oldResetTimeout, newResetTimeout)

  /**
   * Helper method for accessing to the underlying resetTimeout via Unsafe
   */
  @inline
  private[this] def currentResetTimeout: FiniteDuration =
    Unsafe.instance.getObjectVolatile(this, AbstractCircuitBreaker.resetTimeoutOffset).asInstanceOf[FiniteDuration]

  /**
   * Wraps invocations of asynchronous calls that need to be protected
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return [[scala.concurrent.Future]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   */
  def withCircuitBreaker[T](body: ⇒ Future[T], defineFailureFn: Try[T] ⇒ Boolean): Future[T] =
    currentState.invoke(body, defineFailureFn)

  /**
   * Wraps invocations of asynchronous calls that need to be protected
   *
   * @param body Call needing protected
   * @return [[scala.concurrent.Future]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   *
   * 把需要保护的调用用断路器包起来, 使用当前状态的 invoke 方法.
   * 返回值可能是正确的值, 也可能是一个 TimeoutException
   */
  def withCircuitBreaker[T](body: ⇒ Future[T]): Future[T] = currentState.invoke(body, CircuitBreaker.exceptionAsFailure)

  /**
   * Java API for [[#withCircuitBreaker]]
   *
   * @param body Call needing protected
   * @return [[scala.concurrent.Future]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   *
   * Java 接口
   */
  def callWithCircuitBreaker[T](body: Callable[Future[T]]): Future[T] =
    callWithCircuitBreaker(body, CircuitBreaker.exceptionAsFailureJava[T])

  /**
   * Java API for [[#withCircuitBreaker]]
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return [[scala.concurrent.Future]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   */
  def callWithCircuitBreaker[T](body: Callable[Future[T]], defineFailureFn: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean]): Future[T] = {
    val failureFnInScala = CircuitBreaker.convertJavaFailureFnToScala(defineFailureFn)

    withCircuitBreaker(body.call, failureFnInScala)
  }

  /**
   * Java API (8) for [[#withCircuitBreaker]]
   *
   * @param body Call needing protected
   * @return [[java.util.concurrent.CompletionStage]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   *
   * Java 8 接口, 使用 [[java.util.concurrent.CompletionStage]]
   * 通过 [[FutureConverters]] 对 Future 和 CompletionStage 进行转换
   */
  def callWithCircuitBreakerCS[T](body: Callable[CompletionStage[T]]): CompletionStage[T] =
    callWithCircuitBreakerCS(body, CircuitBreaker.exceptionAsFailureJava)

  /**
   * Java API (8) for [[#withCircuitBreaker]]
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return [[java.util.concurrent.CompletionStage]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   */
  def callWithCircuitBreakerCS[T](
    body:            Callable[CompletionStage[T]],
    defineFailureFn: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean]): CompletionStage[T] =
    FutureConverters.toJava[T](callWithCircuitBreaker(new Callable[Future[T]] {
      override def call(): Future[T] = FutureConverters.toScala(body.call())
    }, defineFailureFn))

  /**
   * Wraps invocations of synchronous calls that need to be protected
   *
   * Calls are run in caller's thread. Because of the synchronous nature of
   * this call the  `scala.concurrent.TimeoutException` will only be thrown
   * after the body has completed.
   *
   * Throws java.util.concurrent.TimeoutException if the call timed out.
   *
   * @param body Call needing protected
   * @return The result of the call
   *
   * 把需要保护的同步调用用断路器包起来
   */
  def withSyncCircuitBreaker[T](body: ⇒ T): T =
    withSyncCircuitBreaker(body, CircuitBreaker.exceptionAsFailure)

  /**
   * Wraps invocations of synchronous calls that need to be protected
   *
   * Calls are run in caller's thread. Because of the synchronous nature of
   * this call the  `scala.concurrent.TimeoutException` will only be thrown
   * after the body has completed.
   *
   * Throws java.util.concurrent.TimeoutException if the call timed out.
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return The result of the call
   */
  def withSyncCircuitBreaker[T](body: ⇒ T, defineFailureFn: Try[T] ⇒ Boolean): T =
    Await.result(
      withCircuitBreaker(
        try Future.successful(body) catch { case NonFatal(t) ⇒ Future.failed(t) },
        defineFailureFn),
      callTimeout)

  /**
   * Java API for [[#withSyncCircuitBreaker]]. Throws [[java.util.concurrent.TimeoutException]] if the call timed out.
   *
   * @param body Call needing protected
   * @return The result of the call
   */
  def callWithSyncCircuitBreaker[T](body: Callable[T]): T =
    callWithSyncCircuitBreaker(body, CircuitBreaker.exceptionAsFailureJava[T])

  /**
   * Java API for [[#withSyncCircuitBreaker]]. Throws [[java.util.concurrent.TimeoutException]] if the call timed out.
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return The result of the call
   */
  def callWithSyncCircuitBreaker[T](body: Callable[T], defineFailureFn: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean]): T = {
    val failureFnInScala = CircuitBreaker.convertJavaFailureFnToScala(defineFailureFn)
    withSyncCircuitBreaker(body.call, failureFnInScala)
  }

  /**
   * Mark a successful call through CircuitBreaker. Sometimes the callee of CircuitBreaker sends back a message to the
   * caller Actor. In such a case, it is convenient to mark a successful call instead of using Future
   * via [[withCircuitBreaker]]
   */
  def succeed(): Unit = {
    currentState.callSucceeds()
  }

  /**
   * Mark a failed call through CircuitBreaker. Sometimes the callee of CircuitBreaker sends back a message to the
   * caller Actor. In such a case, it is convenient to mark a failed call instead of using Future
   * via [[withCircuitBreaker]]
   */
  def fail(): Unit = {
    currentState.callFails()
  }

  /**
   * Return true if the internal state is Closed. WARNING: It is a "power API" call which you should use with care.
   * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
   * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
   * manage the state yourself.
   */
  def isClosed: Boolean = {
    currentState == Closed
  }

  /**
   * Return true if the internal state is Open. WARNING: It is a "power API" call which you should use with care.
   * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
   * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
   * manage the state yourself.
   */
  def isOpen: Boolean = {
    currentState == Open
  }

  /**
   * Return true if the internal state is HalfOpen. WARNING: It is a "power API" call which you should use with care.
   * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
   * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
   * manage the state yourself.
   */
  def isHalfOpen: Boolean = {
    currentState == HalfOpen
  }

  /**
   * Adds a callback to execute when circuit breaker opens
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   *
   * 添加一个回调函数, 当断路器打开时, 执行该回调函数.
   * 把回调函数包在 Runnable 里, 执行时提交到 ExecutionContext 中的线程池执行
   */
  def onOpen(callback: ⇒ Unit): CircuitBreaker = addOnOpenListener(new Runnable { def run = callback })

  /**
   * Java API for onOpen
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   *
   * 为打开状态添加回调函数
   */
  @deprecated("Use addOnOpenListener instead", "2.5.0")
  def onOpen(callback: Runnable): CircuitBreaker = addOnOpenListener(callback)

  /**
   * Java API for onOpen
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def addOnOpenListener(callback: Runnable): CircuitBreaker = {
    Open addListener callback
    this
  }

  /**
   * Adds a callback to execute when circuit breaker transitions to half-open
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   *
   * 添加一个回调函数, 当断路器进入半开状态时, 执行该回调函数.
   * 把回调函数包在 Runnable 里, 执行时提交到 ExecutionContext 中的线程池执行
   */
  def onHalfOpen(callback: ⇒ Unit): CircuitBreaker = addOnHalfOpenListener(new Runnable { def run = callback })

  /**
   * JavaAPI for onHalfOpen
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  @deprecated("Use addOnHalfOpenListener instead", "2.5.0")
  def onHalfOpen(callback: Runnable): CircuitBreaker = addOnHalfOpenListener(callback)

  /**
   * JavaAPI for onHalfOpen
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   *
   * 为半开状态添加回调函数
   */
  def addOnHalfOpenListener(callback: Runnable): CircuitBreaker = {
    HalfOpen addListener callback
    this
  }

  /**
   * Adds a callback to execute when circuit breaker state closes
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   *
   * 添加一个回调函数, 当断路器进入关闭状态时, 执行该回调函数.
   * 把回调函数包在 Runnable 里, 执行时提交到 ExecutionContext 中的线程池执行
   */
  def onClose(callback: ⇒ Unit): CircuitBreaker = addOnCloseListener(new Runnable { def run = callback })

  /**
   * JavaAPI for onClose
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   *
   * 为关闭状态添加回调函数
   */
  @deprecated("Use addOnCloseListener instead", "2.5.0")
  def onClose(callback: Runnable): CircuitBreaker = addOnCloseListener(callback)

  /**
   * JavaAPI for onClose
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def addOnCloseListener(callback: Runnable): CircuitBreaker = {
    Closed addListener callback
    this
  }

  /**
   * Adds a callback to execute when call finished with success.
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on successful call, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def onCallSuccess(callback: Long ⇒ Unit): CircuitBreaker = addOnCallSuccessListener(new Consumer[Long] {
    def accept(result: Long): Unit = callback(result)
  })

  /**
   * JavaAPI for onCallSuccess
   *
   * @param callback Handler to be invoked on successful call, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def addOnCallSuccessListener(callback: Consumer[Long]): CircuitBreaker = {
    successListeners add callback
    this
  }

  /**
   * Adds a callback to execute when call finished with failure.
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on failed call, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def onCallFailure(callback: Long ⇒ Unit): CircuitBreaker = addOnCallFailureListener(new Consumer[Long] {
    def accept(result: Long): Unit = callback(result)
  })

  /**
   * JavaAPI for onCallFailure
   *
   * @param callback Handler to be invoked on failed call, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def addOnCallFailureListener(callback: Consumer[Long]): CircuitBreaker = {
    callFailureListeners add callback
    this
  }

  /**
   * Adds a callback to execute when call finished with timeout.
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on call finished with timeout, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def onCallTimeout(callback: Long ⇒ Unit): CircuitBreaker = addOnCallTimeoutListener(new Consumer[Long] {
    def accept(result: Long): Unit = callback(result)
  })

  /**
   * JavaAPI for onCallTimeout
   *
   * @param callback Handler to be invoked on call finished with timeout, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def addOnCallTimeoutListener(callback: Consumer[Long]): CircuitBreaker = {
    callTimeoutListeners add callback
    this
  }

  /**
   * Adds a callback to execute when call was failed due to open breaker.
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on call failed due to open breaker.
   * @return CircuitBreaker for fluent usage
   */
  def onCallBreakerOpen(callback: ⇒ Unit): CircuitBreaker = addOnCallBreakerOpenListener(new Runnable { def run = callback })

  /**
   * JavaAPI for onCallBreakerOpen.
   *
   * @param callback Handler to be invoked on call failed due to open breaker.
   * @return CircuitBreaker for fluent usage
   */
  def addOnCallBreakerOpenListener(callback: Runnable): CircuitBreaker = {
    callBreakerOpenListeners add callback
    this
  }

  /**
   * Retrieves current failure count.
   *
   * @return count
   *
   * 当前失败了多少次, 由 Closed 记录
   */
  private[akka] def currentFailureCount: Int = Closed.get

  /**
   * Implements consistent transition between states. Throws IllegalStateException if an invalid transition is attempted.
   *
   * @param fromState State being transitioning from
   * @param toState State being transitioning from
   *
   * 从 fromState 状态过渡到 toState 状态
   */
  private def transition(fromState: State, toState: State): Unit = {
    if (swapState(fromState, toState))
      toState.enter()
    // else some other thread already swapped state
  }

  /**
   * Trips breaker to an open state.  This is valid from Closed or Half-Open states.
   *
   * @param fromState State we're coming from (Closed or Half-Open)
   *
   * 从 [[Closed]] 或 [[HalfOpen]] 状态过渡到 [[Open]] 状态
   */
  private def tripBreaker(fromState: State): Unit = transition(fromState, Open)

  /**
   * Resets breaker to a closed state.  This is valid from an Half-Open state only.
   *
   * 重置断路器, 即从 [[HalfOpen]] 状态转换为 [[Closed]]
   */
  private def resetBreaker(): Unit = transition(HalfOpen, Closed)

  /**
   * Invokes all onSuccess callback handlers.
   *
   * @param start time in nanoseconds of call invocation
   */
  private def notifyCallSuccessListeners(start: Long): Unit = if (!successListeners.isEmpty) {
    val elapsed = System.nanoTime() - start
    val iterator = successListeners.iterator()
    while (iterator.hasNext) {
      val listener = iterator.next()
      executor.execute(new Runnable {
        def run() = listener.accept(elapsed)
      })
    }
  }

  /**
   * Invokes all onCallFailure callback handlers.
   *
   * @param start time in nanoseconds of call invocation
   */
  private def notifyCallFailureListeners(start: Long): Unit = if (!callFailureListeners.isEmpty) {
    val elapsed = System.nanoTime() - start
    val iterator = callFailureListeners.iterator()
    while (iterator.hasNext) {
      val listener = iterator.next()
      executor.execute(new Runnable {
        def run() = listener.accept(elapsed)
      })
    }
  }

  /**
   * Invokes all onCallTimeout callback handlers.
   *
   * @param start time in nanoseconds of call invocation
   */
  private def notifyCallTimeoutListeners(start: Long): Unit = if (!callTimeoutListeners.isEmpty) {
    val elapsed = System.nanoTime() - start
    val iterator = callTimeoutListeners.iterator()
    while (iterator.hasNext) {
      val listener = iterator.next()
      executor.execute(new Runnable {
        def run() = listener.accept(elapsed)
      })
    }
  }

  /**
   * Invokes all onCallBreakerOpen callback handlers.
   */
  private def notifyCallBreakerOpenListeners(): Unit = if (!callBreakerOpenListeners.isEmpty) {
    val iterator = callBreakerOpenListeners.iterator()
    while (iterator.hasNext) {
      val listener = iterator.next()
      executor.execute(listener)
    }
  }

  /**
   * Attempts to reset breaker by transitioning to a half-open state.  This is valid from an Open state only.
   *
   * 通过进入关开状态, 尝试重置断路器.
   */
  private def attemptReset(): Unit = transition(Open, HalfOpen)

  private val timeoutEx = new TimeoutException("Circuit Breaker Timed out.") with NoStackTrace

  private val callFailureListeners = new CopyOnWriteArrayList[Consumer[Long]]

  private val callTimeoutListeners = new CopyOnWriteArrayList[Consumer[Long]]

  private val callBreakerOpenListeners = new CopyOnWriteArrayList[Runnable]

  private val successListeners = new CopyOnWriteArrayList[Consumer[Long]]

  /**
   * Internal state abstraction
   *
   * 断路器的内部状态, 共 3 种:
   * [[Closed]]: 初始状态, 断路器是关的, 正常做异步调用
   * [[Open]]: 当抛出异常个数和超时次数加起来超过设定的阈值, 断路器进入打开状态. 所有的调用都会抛出 [[CircuitBreakerOpenException]] 异常. [[resetTimeout]] 到了之后, 进入半开状态
   * [[HalfOpen]]: 半开状态时, 第一个调用会进行尝试, 其它的则抛出异常快速失败. 如果第一次调用成功, 进入关闭状态; 否则再次进入打开状态, 并等待 resetTimeout 的时间
   */
  private sealed trait State {
    private val listeners = new CopyOnWriteArrayList[Runnable] // 线程安全的 ArrayList

    /**
     * Add a listener function which is invoked on state entry
     *
     * @param listener listener implementation
     *
     * 添加一个 listener
     */
    def addListener(listener: Runnable): Unit = listeners add listener

    /**
     * Test for whether listeners exist
     *
     * @return whether listeners exist
     */
    private def hasListeners: Boolean = !listeners.isEmpty

    /**
     * Notifies the listeners of the transition event via a Future executed in implicit parameter ExecutionContext
     *
     * @return Promise which executes listener in supplied [[scala.concurrent.ExecutionContext]]
     */
    protected def notifyTransitionListeners() {
      if (hasListeners) {
        val iterator = listeners.iterator
        while (iterator.hasNext) {
          val listener = iterator.next
          executor.execute(listener)
        }
      }
    }

    /**
     * Shared implementation of call across all states.  Thrown exception or execution of the call beyond the allowed
     * call timeout is counted as a failed call, otherwise a successful call
     *
     * @param body Implementation of the call
     * @param defineFailureFn function that define what should be consider failure and thus increase failure count
     * @return Future containing the result of the call
     */
    def callThrough[T](body: ⇒ Future[T], defineFailureFn: Try[T] ⇒ Boolean): Future[T] = {

      def materialize[U](value: ⇒ Future[U]): Future[U] = try value catch { case NonFatal(t) ⇒ Future.failed(t) }

      if (callTimeout == Duration.Zero) {
        val start = System.nanoTime()
        val f = materialize(body)

        f.onComplete {
          case s: Success[_] ⇒
            notifyCallSuccessListeners(start)
            callSucceeds()
          case Failure(ex) ⇒
            notifyCallFailureListeners(start)
            callFails()
        }

        f
      } else {
        val start = System.nanoTime()
        val p = Promise[T]()

        implicit val ec = sameThreadExecutionContext

        p.future.onComplete { fResult ⇒
          if (defineFailureFn(fResult)) {
            callFails()
          } else {
            notifyCallSuccessListeners(start)
            callSucceeds() // 调用成功时执行的函数, Closed/Open/HalfOpen 都有相应的实现
          }
        }

        val timeout = scheduler.scheduleOnce(callTimeout) {
          if (p tryFailure timeoutEx) {
            notifyCallTimeoutListeners(start)
          }
        }

        materialize(body).onComplete {
          case Success(result) ⇒
            p.trySuccess(result)
            timeout.cancel
          case Failure(ex) ⇒
            if (p.tryFailure(ex)) {
              notifyCallFailureListeners(start)
            }
            timeout.cancel
        }
        p.future
      }
    }

    /**
     * Shared implementation of call across all states.  Thrown exception or execution of the call beyond the allowed
     * call timeout is counted as a failed call, otherwise a successful call
     *
     * @param body Implementation of the call
     * @return Future containing the result of the call
     */
    def callThrough[T](body: ⇒ Future[T]): Future[T] = callThrough(body, CircuitBreaker.exceptionAsFailure)

    /**
     * Abstract entry point for all states
     *
     * @param body Implementation of the call that needs protected
     * @param defineFailureFn function that define what should be consider failure and thus increase failure count
     * @return Future containing result of protected call
     */
    def invoke[T](body: ⇒ Future[T], defineFailureFn: Try[T] ⇒ Boolean): Future[T]

    /**
     * Abstract entry point for all states
     *
     * @param body Implementation of the call that needs protected
     * @return Future containing result of protected call
     */
    def invoke[T](body: ⇒ Future[T]): Future[T] = invoke(body, CircuitBreaker.exceptionAsFailure)

    /**
     * Invoked when call succeeds
     *
     * 调用成功时, 执行该函数. 每个状态会有自己的实现
     */
    def callSucceeds(): Unit

    /**
     * Invoked when call fails
     *
     * 调用失败时, 执行该函数. 每个状态会有自己的实现
     */
    def callFails(): Unit

    /**
     * Invoked on the transitioned-to state during transition.  Notifies listeners after invoking subclass template
     * method _enter
     *
     * 进入具体的一个状态时调用 _enter() 进行设置, 然后调用已注册的监听函数
     */
    final def enter(): Unit = {
      _enter()
      notifyTransitionListeners()
    }

    /**
     * Template method for concrete traits
     *
     */
    def _enter(): Unit
  }

  /**
   * Concrete implementation of Closed state
   * 断路器的关闭状态, 也是默认状态. 继承 [[AtomicInteger]], 将自身视为一个失败事件计数器. 每次调用失败会加 1, 直到设定的失败阈值.
   */
  private object Closed extends AtomicInteger with State {

    /**
     * Implementation of invoke, which simply attempts the call
     *
     * @param body Implementation of the call that needs protected
     * @return Future containing result of protected call
     *
     * 处于关闭状态时, 直接进行异步调用.
     */
    override def invoke[T](body: ⇒ Future[T], defineFailureFn: Try[T] ⇒ Boolean): Future[T] =
      callThrough(body, defineFailureFn)

    /**
     * On successful call, the failure count is reset to 0
     *
     * @return
     *
     * 只要调用成功, 则将计数器置 0
     */
    override def callSucceeds(): Unit = set(0)

    /**
     * On failed call, the failure count is incremented.  The count is checked against the configured maxFailures, and
     * the breaker is tripped if we have reached maxFailures.
     *
     * @return
     *
     * 调用失败, 计数器增 1, 并且与设定的最大失败次数进行对比, 达到了则阈值则进入 [[Open]] 状态.
     */
    override def callFails(): Unit = if (incrementAndGet() == maxFailures) tripBreaker(Closed)

    /**
     * On entry of this state, failure count and resetTimeout is reset.
     *
     * @return
     *
     * 进入该状态时, 计数器清 0
     */
    override def _enter(): Unit = {
      set(0)
      swapResetTimeout(currentResetTimeout, resetTimeout)
    }

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = "Closed with failure count = " + get()
  }

  /**
   * Concrete implementation of half-open state
   *
   * 断路器的半开状态, [[Open]] 状态在达到 [[resetTimeout]] 后会进入该状态.
   * 半开状态时, 第一个调用会进行尝试, 其它的则抛出异常快速失败. 如果第一调用成功, 进入关闭状态; 否则再次进入打开状态, 并等待 resetTimeout 的时间.
   * 继承 [[AtomicBoolean]] 并设置初始值为 true, 第一调用后设置为 false, 以此达到只调用一次的效果.
   */
  private object HalfOpen extends AtomicBoolean(true) with State {

    /**
     * Allows a single call through, during which all other callers fail-fast.  If the call fails, the breaker reopens.
     * If the call succeeds the breaker closes.
     *
     * @param body Implementation of the call that needs protected
     * @return Future containing result of protected call
     *
     * 第一次调用会进行尝试, 其它的则抛出异常快速失败.
     * 如果第一次调用成功, 进入关闭状态; 否则再次进入打开状态, 并等待 resetTimeout 的时间.
     */
    override def invoke[T](body: ⇒ Future[T], defineFailureFn: Try[T] ⇒ Boolean): Future[T] =
      if (compareAndSet(true, false))
        callThrough(body, defineFailureFn)
      else {
        notifyCallBreakerOpenListeners()
        Promise.failed[T](new CircuitBreakerOpenException(0.seconds)).future
      }

    /**
     * Reset breaker on successful call.
     *
     * @return
     *
     * 如果第一次调用成功, 进入关闭状态
     */
    override def callSucceeds(): Unit = resetBreaker()

    /**
     * Reopen breaker on failed call.
     *
     * @return
     *
     * 如果第一次调用失败, 进入打开状态
     */
    override def callFails(): Unit = tripBreaker(HalfOpen)

    /**
     * On entry, guard should be reset for that first call to get in
     *
     * @return
     *
     * 进入该状态时, 设置初始布尔值为 true
     */
    override def _enter(): Unit = set(true)

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = "Half-Open currently testing call for success = " + get()
  }

  /**
   * Concrete implementation of Open state
   *
   * Open 继承了 [[AtomicLong]], 将自身视为时间戳, 进入该状态时, 会将当前时间戳设置到自身, 表示设置这个状态的起始时间
   */
  private object Open extends AtomicLong with State {

    /**
     * Fail-fast on any invocation
     *
     * @param body Implementation of the call that needs protected
     * @return Future containing result of protected call
     *
     * 断路器处于打开状态时, 所有的调用都会快速失败
     */
    override def invoke[T](body: ⇒ Future[T], defineFailureFn: Try[T] ⇒ Boolean): Future[T] = {
      notifyCallBreakerOpenListeners()
      Promise.failed[T](new CircuitBreakerOpenException(remainingDuration())).future
    }

    /**
     * Calculate remaining duration until reset to inform the caller in case a backoff algorithm is useful
     *
     * @return duration to when the breaker will attempt a reset by transitioning to half-open
     *
     * 返回离到达 [[resetTimeout]] 还剩余的时间
     */
    private def remainingDuration(): FiniteDuration = {
      val fromOpened = System.nanoTime() - get // Open 状态已经持续的时间
      val diff = currentResetTimeout.toNanos - fromOpened // 离到达 resetTimeout 还有多少时间
      if (diff <= 0L) Duration.Zero // 如果小于 0, 则到达 resetTimeout
      else diff.nanos // 否则返回剩余时间
    }

    /**
     * No-op for open, calls are never executed so cannot succeed or fail
     *
     * @return
     *
     * 处于 Open 状态时, 不会执行调用, 因此无需相应的处于函数
     */
    override def callSucceeds(): Unit = ()

    /**
     * No-op for open, calls are never executed so cannot succeed or fail
     *
     * @return
     *
     * 同 [[callSucceeds]]
     */
    override def callFails(): Unit = ()

    /**
     * On entering this state, schedule an attempted reset via [[akka.actor.Scheduler]] and store the entry time to
     * calculate remaining time before attempted reset.
     *
     * @return
     *
     * 用当前时间戳设置进入该状态时的时间, 并起一个定时任务, 在 [[resetTimeout]] 后尝试重置断路器, 即把状态改为半开状态
     */
    override def _enter(): Unit = {
      set(System.nanoTime())
      scheduler.scheduleOnce(currentResetTimeout) {
        attemptReset()
      }
      val nextResetTimeout = currentResetTimeout * exponentialBackoffFactor match {
        case f: FiniteDuration ⇒ f
        case _                 ⇒ currentResetTimeout
      }

      if (nextResetTimeout < maxResetTimeout)
        swapResetTimeout(currentResetTimeout, nextResetTimeout)
    }

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = "Open"
  }

}

/**
 * Exception thrown when Circuit Breaker is open.
 *
 * @param remainingDuration Stores remaining time before attempting a reset.  Zero duration means the breaker is
 *                          currently in half-open state.
 * @param message Defaults to "Circuit Breaker is open; calls are failing fast"
 *
 * 当断路器处于打开状态时, 所有的调用都会抛出以下异常
 * 当断路器处于半开状态时, 除了第一个调用, 其它调用都会抛出以下异常
 */
class CircuitBreakerOpenException(
  val remainingDuration: FiniteDuration,
  message:               String         = "Circuit Breaker is open; calls are failing fast")
  extends AkkaException(message) with NoStackTrace
