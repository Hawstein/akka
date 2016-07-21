/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.serialization

import akka.actor.{ ActorSystem, ExtensionId, ExtensionIdProvider, ExtendedActorSystem }
// Done by Hawstein
/**
 * SerializationExtension is an Akka Extension to interact with the Serialization
 * that is built into Akka
 *
 * Akka 的序列化扩展
 */
object SerializationExtension extends ExtensionId[Serialization] with ExtensionIdProvider {
  override def get(system: ActorSystem): Serialization = super.get(system)
  override def lookup = SerializationExtension
  override def createExtension(system: ExtendedActorSystem): Serialization = new Serialization(system)
}
