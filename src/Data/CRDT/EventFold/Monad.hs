{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wmissing-import-lists #-}

{- | Description: Monadic interaction with an EventFold. -}
module Data.CRDT.EventFold.Monad (
  MonadEventFold(..),
  EventFoldT,
  runEventFoldT,
) where


import Control.Monad.Reader (MonadReader(ask), ReaderT(runReaderT))
import Control.Monad.State (MonadState(state), StateT, runStateT)
import Control.Monad.Trans.Class (MonadTrans(lift))
import Data.CRDT.EventFold (Event(Output), UpdateResult(UpdateResult),
  Diff, EventFold, EventId, MergeError)
import qualified Data.CRDT.EventFold as EF (diffMerge, disassociate,
  event, fullMerge, participate)


{- |
  The interface for monadically updating an EventFold, where the
  monadic context is intended to manage:

  - The local participant.
  - The current state of the EventFold.
  - The accumulated consistent outputs.
  - Whether the 'EventFold' needs to be propagated to other participants.
-}
class MonadEventFold o p e m | m -> o p e where
  {- | Apply an event. See 'EF.event'. -}
  event :: e -> m (Output e, EventId p)

  {- | Perform a full merge. See 'EF.fullMerge'. -}
  fullMerge
    :: EventFold o p e
    -> m (Either (MergeError o p e) ())

  {- | Perform a diff merge. See 'EF.diffMerge'. -}
  diffMerge
    :: Diff o p e
    -> m (Either (MergeError o p e) ())

  {- | Allow a new participant to join in the cluster. See 'EF.participate'. -}
  participate :: p -> m (EventId p)

  {- | Remove a peer from participation. See 'EF.disassociate'. -}
  disassociate :: p -> m (EventId p)

{- | A transformer providing 'MonadEventFold'. -}
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
    )
instance MonadTrans (EventFoldT o p e) where
  lift = EventFoldT . lift . lift

instance
    ( Eq (Output e)
    , Eq e
    , Eq o
    , Event e
    , Monad m
    , Ord p
    )
  =>
    MonadEventFold o p e (EventFoldT o p e m)
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
runEventFoldT self ef action = do
  flip runReaderT self
  . flip runStateT (UpdateResult ef mempty False)
  . unEventFoldT 
  $ action

