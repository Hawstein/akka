/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.actor

import scala.collection.immutable
import scala.reflect.ClassTag
import scala.util.Try

// Done by Hawstein
/**
 * The DynamicAccess implementation is the class which is used for
 * loading all configurable parts of an actor system (the
 * [[akka.actor.ReflectiveDynamicAccess]] is the default implementation).
 *
 * This is an internal facility and users are not expected to encounter it
 * unless they are extending Akka in ways which go beyond simple Extensions.
 *
 * 该类用来加载 actor system 的配置部分, 默认实现: [[akka.actor.ReflectiveDynamicAccess]]
 */
abstract class DynamicAccess {
  /**
   * Convenience method which given a `Class[_]` object and a constructor description
   * will create a new instance of that class.
   *
   * {{{
   * val obj = DynamicAccess.createInstanceFor(clazz, Seq(classOf[Config] -> config, classOf[String] -> name))
   * }}}
   *
   * 给定一个类型, 及创建该类型实例所需要的参数, 来得到该类型的一个实例
   */
  def createInstanceFor[T: ClassTag](clazz: Class[_], args: immutable.Seq[(Class[_], AnyRef)]): Try[T]

  /**
   * Obtain a `Class[_]` object loaded with the right class loader (i.e. the one
   * returned by `classLoader`).
   *
   * fqcn: Fully qualified class name
   * 给出一个完全限定类名的字符串, 返回一个类型
   */
  def getClassFor[T: ClassTag](fqcn: String): Try[Class[_ <: T]]

  /**
   * Obtain an object conforming to the type T, which is expected to be
   * instantiated from a class designated by the fully-qualified class name
   * given, where the constructor is selected and invoked according to the
   * `args` argument. The exact usage of args depends on which type is requested,
   * see the relevant requesting code for details.
   *
   * 给定一个完全限定类名字符串, 及创建该类型实例所需要的参数, 来得到该类型的一个实例
   */
  def createInstanceFor[T: ClassTag](fqcn: String, args: immutable.Seq[(Class[_], AnyRef)]): Try[T]

  /**
   * Obtain the Scala “object” instance for the given fully-qualified class name, if there is one.
   * 给定一个完全限定类名字符串, 得到一个 Scala object
   */
  def getObjectFor[T: ClassTag](fqcn: String): Try[T]

  /**
   * This is the class loader to be used in those special cases where the
   * other factory method are not applicable (e.g. when constructing a ClassLoaderBinaryInputStream).
   */
  def classLoader: ClassLoader
}
