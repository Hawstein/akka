/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence

import akka.actor._
import akka.persistence.SnapshotProtocol._

/**
 * Snapshot API on top of the internal snapshot protocol.
 */
trait Snapshotter extends Actor {

  /**
   * 具体实现在 [[Eventsourced.snapshotStore]]
   */
  /** Snapshot store plugin actor. */
  private[persistence] def snapshotStore: ActorRef

  /**
   * Snapshotter id.
   */
  def snapshotterId: String

  /**
   * Sequence number to use when taking a snapshot.
   */
  def snapshotSequenceNr: Long

  /**
   * Instructs the snapshot store to load the specified snapshot and send it via an [[SnapshotOffer]]
   * to the running [[PersistentActor]].
   *
   * 向 snapshotStore 发送 LoadSnapshot, 令其加载指定的 snapshot 并通过 [[SnapshotOffer]] 发给同一上下文中的 PersistentActor
   * snapshotStore 实例在 [[Eventsourced.snapshotStore]]
   * 具体的实现会继承 [[akka.persistence.snapshot.SnapshotStore]] trait
   */
  def loadSnapshot(persistenceId: String, criteria: SnapshotSelectionCriteria, toSequenceNr: Long) =
    snapshotStore ! LoadSnapshot(persistenceId, criteria, toSequenceNr)

  /**
   * Saves a `snapshot` of this snapshotter's state.
   *
   * The [[PersistentActor]] will be notified about the success or failure of this
   * via an [[SaveSnapshotSuccess]] or [[SaveSnapshotFailure]] message.
   */
  def saveSnapshot(snapshot: Any): Unit = {
    snapshotStore ! SaveSnapshot(SnapshotMetadata(snapshotterId, snapshotSequenceNr), snapshot)
  }

  /**
   * Deletes the snapshot identified by `sequenceNr`.
   *
   * The [[PersistentActor]] will be notified about the status of the deletion
   * via an [[DeleteSnapshotSuccess]] or [[DeleteSnapshotFailure]] message.
   */
  def deleteSnapshot(sequenceNr: Long): Unit = {
    snapshotStore ! DeleteSnapshot(SnapshotMetadata(snapshotterId, sequenceNr))
  }

  /**
   * Deletes all snapshots matching `criteria`.
   *
   * The [[PersistentActor]] will be notified about the status of the deletion
   * via an [[DeleteSnapshotsSuccess]] or [[DeleteSnapshotsFailure]] message.
   */
  def deleteSnapshots(criteria: SnapshotSelectionCriteria): Unit = {
    snapshotStore ! DeleteSnapshots(snapshotterId, criteria)
  }

}
