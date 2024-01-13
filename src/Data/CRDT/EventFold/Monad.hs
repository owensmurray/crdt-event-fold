{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wmissing-import-lists #-}

{- | Description: Monadic interaction with an EventFold. -}
module Data.CRDT.EventFold.Monad (
  MonadUpdateEF(..),
  MonadInspectEF(..),
  EventFoldT,
  runEventFoldT,
) where


import Control.Monad.Catch (MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Logger (MonadLogger, MonadLoggerIO)
import Control.Monad.Reader (MonadReader(ask), ReaderT(runReaderT))
import Control.Monad.State (MonadState(get, state), StateT(runStateT),
  gets)
import Control.Monad.Trans.Class (MonadTrans(lift))
import Data.CRDT.EventFold (Event(Output), UpdateResult(UpdateResult,
  urEventFold), Diff, EventFold, EventId, MergeError)
import Prelude (Bool(False), Either(Left, Right), Monoid(mempty),
  Semigroup((<>)), ($), (.), (<$>), (=<<), (||), Applicative, Eq, Functor,
  Monad, Ord, flip, id)
import qualified Data.CRDT.EventFold as EF


{- |
  The interface for monadically updating an EventFold, where the
  monadic context is intended to manage:

  - The local participant.
  - The current state of the EventFold.
  - The accumulated consistent outputs.
  - Whether the 'EventFold' needs to be propagated to other participants.
-}
class MonadUpdateEF o p e m | m -> o p e where
  {- | Apply an event. See 'EF.event'. -}
  event :: e -> m (Output e, EventId p)

  {- | Perform a full merge. See 'EF.fullMerge'. -}
  fullMerge :: EventFold o p e -> m (Either (MergeError o p e) ())

  {- | Perform a diff merge. See 'EF.diffMerge'. -}
  diffMerge :: Diff o p e -> m (Either (MergeError o p e) ())

  {- | Allow a new participant to join in the cluster. See 'EF.participate'. -}
  participate :: p -> m (EventId p)

  {- | Remove a peer from participation. See 'EF.disassociate'. -}
  disassociate :: p -> m (EventId p)

  {- | Get the outstanding update results. -}
  getResult :: m (UpdateResult o p e)


{- |
  Interface for inspecting an Eventfold contained within the monadic
  context.
-}
class (Monad m) => MonadInspectEF o p e m | m -> o p e where
  efAsks :: (EventFold o p e -> a) -> m a
  efAsks f = f <$> efAsk

  efAsk :: m (EventFold o p e)
  efAsk = efAsks id


{- | A transformer providing 'MonadUpdateEF' and 'MonadInspectEF'. -}
newtype EventFoldT o p e m a = EventFoldT {
    unEventFoldT ::
      StateT (UpdateResult o p e) (
      ReaderT p m)
      a
  }
  deriving newtype
    ( Applicative
    , Functor
    , Monad
    , MonadIO
    , MonadLogger
    , MonadLoggerIO
    , MonadThrow
    )
instance MonadTrans (EventFoldT o p e) where
  lift = EventFoldT . lift . lift

instance (Monad m) => MonadInspectEF o p e (EventFoldT o p e m) where
  efAsks f = EventFoldT $ gets (f . urEventFold)
  efAsk = EventFoldT $ gets urEventFold

instance
    ( Eq (Output e)
    , Eq e
    , Eq o
    , Event p e
    , Monad m
    , Ord p
    )
  =>
    MonadUpdateEF o p e (EventFoldT o p e m)
  where
    event e =
      withEF
        (\ef self ->
          let (o, eid, ur) = EF.event self e ef
          in ((o, eid), ur)
        )

    fullMerge other =
      withEF
        (\ef self ->
          case EF.fullMerge self ef other of
            Left err -> (Left err, UpdateResult ef mempty False)
            Right ur -> (Right (), ur)
        )

    diffMerge diff =
      withEF
        (\ef self ->
          case EF.diffMerge self ef diff of
            Left err -> (Left err, UpdateResult ef mempty False)
            Right ur -> (Right (), ur)
        )

    participate participant =
      withEF (\ef self -> EF.participate self participant ef)

    disassociate participant =
      withEF (\ef _self -> EF.disassociate participant ef)

    getResult = EventFoldT get


{- |
  EventFoldT helper to make sure we always do the right thing when
  updating an event.
-}
withEF
  :: forall o p e m a. (Monad m, Ord p)
  => (EventFold o p e -> p -> (a, UpdateResult o p e))
  -> EventFoldT o p e m a
withEF f = EventFoldT $
    state . updateState =<< ask
  where
    updateState :: p -> UpdateResult o p e -> (a, UpdateResult o p e)
    updateState self (UpdateResult ef outputs prop) =
      let
        (a, UpdateResult ef2 outputs2 prop2) =
          f ef self
        results :: UpdateResult o p e
        results = UpdateResult ef2 (outputs <> outputs2) (prop || prop2)
      in
        (a, results)

runEventFoldT
  :: (Ord p)
  => p {- ^ The local participant. -}
  -> EventFold o p e {- ^ Initial event fold value.  -}
  -> EventFoldT o p e m a {- ^ The action to run.  -}
  -> m (a, UpdateResult o p e)
     {- ^
       Returns the result of the action, plus all the accumulated
       'UpdateResult's, which contain the new 'EventFold' value, all
       of the consistent outputs, and a flag indicating whether the new
       'EventFold' value should be propagated to the other participants.
     -}
runEventFoldT self ef =
  flip runReaderT self
  . flip runStateT (UpdateResult ef mempty False)
  . unEventFoldT

