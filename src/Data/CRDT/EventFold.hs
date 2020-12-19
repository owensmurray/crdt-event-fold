{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wmissing-deriving-strategies #-}
{-# OPTIONS_GHC -Wmissing-import-lists #-}

{- | Description: Garbage collected event folding CRDT. -}
module Data.CRDT.EventFold (
  -- * Overview
  {- |
    This module provides a CRDT data structure that collects and applies
    operations (called "events") that mutate an underlying data structure.

    It is "Garbage Collected" in the sense that the number of operations
    accumulated in the structure will not grow unbounded, assuming that
    participants manage to sync their data once in a while. The size of
    the data (as measured by the number of operations we have to store)
    is allowed to shrink.

    In addition to mutating the underlying data, each operation can
    also produce an output that can be obtained by the client. The
    output can be either totally consistent across all replicas (which
    is slower), or it can be returned immediately and possibly reflect
    an inconsistent state.
  -}

  -- ** Garbage Collection
  {- |
    Unlike many traditional CRDTs which always grow and never shrink,
    'EventFold' has a mechanism for determining what consensus
    has been reached by all of the participants, which allows us to
    "garbage collect" events that achieved total consensus. Perhaps more
    importantly, this allows us to produce the totally consistent output
    for events for which total consensus has been achieved.

    But there are trade offs. The big downside is that participation in the
    distributed replication of the 'EventFold' must be strictly managed.

    - The process of participating itself involves registering with an
      existing participant, using 'participate'. You can't just send the
      data off to some other computer and expect that now that computer
      is participating in the CRDT. It isn't.
    - Participants can not "restore from backup". Once they have
      incorporated data received from other participants or generated
      new data themselves, and that data has been transmitted to any
      other participant, they are committed to using that result going
      forward. Doing anything that looks like "restoring from an older
      version" would destroy the idea that participants have reached
      consensus on anything, and the results would be undefined and
      almost certainly completely wrong. This library is written with
      some limited capability to detect this situation, but it is not
      always possible to detect it all cases. Many times you will just
      end up with undefined behavior.
  -}

  -- ** A Belabored Analogy
  {- |
    The 'EventFold' name derives from a loose analogy to folding over
    a list of events using plain old 'foldl'. The component parts of
    'foldl' are:

    - A binary operator, analogous to 'apply'.

    - An accumulator value, analogous to 'infimumValue'.

    - A list of values to fold over, loosely analogous to "the list of
      all future calls to 'event'".

    - A return value.  There is no real analogy for the "return value".
      Similarly to how you never actually obtain a return value if you
      try to 'foldl' over an infinite list, 'EventFold's are meant to be
      long-lived objects that accommodate an infinite number of calls
      to 'event'. What you can do is inspect the current value of the
      accumulator using 'infimumValue', or the "projected" value of
      the accumulator using 'projectedValue' (where "projected" means
      "taking into account all of the currently known calls to 'event'
      that have not yet been folded into the accumulator, and which may
      yet turn out to to have other events inserted into the middle or
      beginning of the list").

    The 'EventFold' value itself can be thought of as an intermediate,
    replicated, current state of the fold of an infinite list of events
    that has not yet been fully generated.  So you can, for instance,
    check the current accumulator value.

    In a little more detail, consider the type signature of 'foldl'
    (for lists).

    > foldl
    >   :: (b -> a -> b) -- Analogous to 'apply', where 'a' is your 'Event'
    >                    -- instance, and 'b' is 'State a'.
    >
    >   -> b             -- Loosely analogous to 'infimumValue' where
    >                    -- progressive applications are accumulated.
    >
    >   -> [a]           -- Analogous to all outstanding or future calls to
    >                    -- 'event'.
    >
    >   -> b             
  -}
  -- * Basic API
  -- ** Creating new CRDTs
  new,

  -- ** Adding new events
  event,

  -- ** Coordinating replica updates
  {- |
    Functions in this section are used to help merge foreign copies of
    the CRDT, and transmit our own copy. (This library does not provide
    any kind of transport support, except that all the relevant types
    have 'Binary' instances. Actually arranging for these things to get
    shipped across a wire is left to the user.)

    In principal, the only function you need is 'fullMerge'. Everything
    else in this section is an optimization.  You can ship the full
    'EventFold' value to a remote participant and it can incorporate
    any changes using 'fullMerge', and vice versa. You can receive an
    'EventFold' value from another participant and incorporate its
    changes locally using 'fullMerge'.

    However, if your underlying data structure is large, it may be more
    efficient to just ship a sort of diff containing the information
    that the local participant thinks the remote participant might be
    missing. That is what 'events' and 'diffMerge' are for.
  -}
  fullMerge,
  UpdateResult(..),
  events,
  diffMerge,
  MergeError(..),

  -- ** Participation
  participate,
  disassociate,

  -- ** Defining your state and events
  Event(..),
  EventResult(..),

  -- * Inspecting the 'EventFold'
  isBlockedOnError,
  projectedValue,
  infimumValue,
  infimumId,
  infimumParticipants,
  allParticipants,
  projParticipants,
  origin,
  divergent,

  -- * Underlying Types
  EventFold,
  EventId,
  Diff,

) where


import Data.Bifunctor (first)
import Data.Binary (Binary(get, put))
import Data.Default.Class (Default(def))
import Data.Functor.Identity (Identity(Identity), runIdentity)
import Data.Map (Map, keys, toAscList, toDescList, unionWith)
import Data.Maybe (catMaybes)
import Data.Set ((\\), Set, member, union)
import GHC.Generics (Generic)
import qualified Data.DoubleWord as DW
import qualified Data.Map as Map
import qualified Data.Map.Merge.Lazy as Map.Merge
import qualified Data.Set as Set


data EventFoldF o p e f = EventFoldF {
     psOrigin :: o,
    psInfimum :: Infimum (State e) p,
     psEvents :: Map (EventId p) (f (Delta p e), Set p)
  }
  deriving stock (Generic)
deriving stock instance
    ( Eq (f (Delta p e))
    , Eq (Output e)
    , Eq o
    , Eq p
    , Eq e
    )
  =>
    Eq (EventFoldF o p e f)
instance
    (
      Binary (f (Delta p e)),
      Binary o,
      Binary p,
      Binary e,
      Binary (State e),
      Binary (Output e)
    )
  =>
    Binary (EventFoldF o p e f)
deriving stock instance
    ( Show (f (Delta p e))
    , Show o
    , Show p
    , Show (State e)
    )
  => Show (EventFoldF o p e f)


{- |
  This type is a
  <https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type CRDT>
  into which participants can add 'Event's that are folded into a
  base 'State'. You can also think of the "events" as operations that
  mutate the base state, and the point of this CRDT is to coordinate
  the application of the operations across all participants so that
  they are applied consistently even if the operations themselves are
  not commutative, idempotent, or monotonic.

  Variables are:

  - @o@ - Origin
  - @p@ - Participant
  - @e@ - Event

  The "Origin" is a value that is more or less meant to identify the
  "thing" being replicated, and in particular identify the historical
  lineage of the 'EventFold'. The idea is that it is meaningless to
  try and merge two 'EventFold's that do not share a common history
  (identified by the origin value) and doing so is a programming error. It
  is only used to try and check for this type of programming error and
  throw an exception if it happens instead of producing undefined (and
  difficult to detect) behavior.
-}
newtype EventFold o p e = EventFold { unEventFold :: EventFoldF o p e Identity}
deriving stock instance
    (Show o, Show p, Show e, Show (Output e), Show (State e))
  =>
    Show (EventFold o p e)
deriving newtype instance
    (Binary o, Binary p, Binary e, Binary (Output e), Binary (State e))
  =>
    Binary (EventFold o p e)
deriving newtype instance
    (Eq o, Eq p, Eq e, Eq (Output e))
  =>
    Eq (EventFold o p e)


{- |
  `Infimum` is the infimum, or greatest lower bound, of the possible
  values of @s@.
-}
data Infimum s p = Infimum {
         eventId :: EventId p,
    participants :: Set p,
      stateValue :: s
  }
  deriving stock (Generic, Show)
instance (Binary s, Binary p) => Binary (Infimum s p)
instance (Eq p) => Eq (Infimum s p) where
  Infimum s1 _ _ == Infimum s2 _ _ = s1 == s2
instance (Ord p) => Ord (Infimum s p) where
  compare (Infimum s1 _ _) (Infimum s2 _ _) = compare s1 s2


{- |
  `EventId` is a monotonically increasing, totally ordered identification
  value which allows us to lend the attribute of monotonicity to event
  application operations which would not naturally be monotonic.
-}
data EventId p
  = BottomEid
  | Eid Word256 p
  deriving stock (Generic, Eq, Ord, Show)
  deriving anyclass (Binary)
instance Default (EventId p) where
  def = BottomEid


{- | Newtype around 'DW.Word256' to supply typeclass instances. -}
newtype Word256 = Word256 {
    unWord256 :: DW.Word256
  }
  deriving stock (Generic)
  deriving newtype (Eq, Ord, Show, Enum, Num)
instance Binary Word256 where
  put (Word256 (DW.Word256 (DW.Word128 a b) (DW.Word128 c d))) =
    put (a, b, c, d)
  get = do
    (a, b, c, d) <- get
    pure (Word256 (DW.Word256 (DW.Word128 a b) (DW.Word128 c d)))


{- |
  This is the exception type for illegal merges. These errors indicate
  serious programming bugs.
-}
data MergeError o p e
  = DifferentOrigins o o
    {- ^
      The 'EventFold's do not have the same origin. It makes no sense
      to merge 'EventFold's that have different origins because they
      do not share a common history.
    -}
  | DiffTooNew (EventFold o p e) (Diff o p e)
    {- ^
      The `Diff`'s infimum is greater than any event known to 'EventFold'
      into which it is being merged. This should be impossible and
      indicates that either the local 'EventFold' has rolled back an
      event that it had previously acknowledged, or else the source of
      the 'Diff' moved the infimum forward without a full acknowledgement
      from all participants. Both of these conditions should be regarded
      as serious bugs.
    -}
  | DiffTooSparse (EventFold o p e) (Diff o p e)
    {- ^
      The 'Diff' assumes we know about events that we do not in fact know
      about. This is only possible if we rolled back our copy of the state
      somehow and "forgot" about state that we had previous acknowledged,
      or else some other participant erroneously acknowledged some events
      on our behalf.
    -}
deriving stock instance
    ( Show (Output e)
    , Show o
    , Show p
    , Show e
    , Show (State e)
    )
  =>
    Show (MergeError o p e)


{- | `Delta` is how we represent mutations to the event fold state. -}
data Delta p e
  = Join p
  | UnJoin p
  | Event e
  | Error (Output e) (Set p)
  deriving stock (Generic)
deriving stock instance (Eq p, Eq e, Eq (Output e)) => Eq (Delta p e)
deriving stock instance (Show p, Show e, Show (Output e)) => Show (Delta p e)
instance (Binary p, Binary e, Binary (Output e)) => Binary (Delta p e)


{- |
  Instances of this class define the particular "events" being "folded"
  over in a distributed fashion. In addition to the event type itself,
  there are a couple of type families which define the 'State' into which
  folded events are accumulated, and the 'Output' which application of
  a particular event can generate.

  TL;DR: This is how users define their own custom operations.
-}
class Event e where
  type Output e
  type State e
  {- | Apply an event to a state value. **This function MUST be total!!!** -}
  apply :: e -> State e -> EventResult e
{- | The most trivial event type. -}
instance Event () where
  type Output () = ()
  type State () = ()
  apply () () = Pure () ()
{- | The union of two event types. -}
instance (Event a, Event b) => Event (Either a b) where
  type Output (Either a b) = Either (Output a) (Output b)
  type State (Either a b) = (State a, State b)

  apply (Left e) (a, b) = 
    case apply e a of
      SystemError o -> SystemError (Left o)
      Pure o s -> Pure (Left o) (s, b)
  apply (Right e) (a, b) = 
    case apply e b of
      SystemError o -> SystemError (Right o)
      Pure o s -> Pure (Right o) (a, s)


{- |
  The result of applying an event.

  Morally speaking, events are always pure functions. However, mundane
  issues like finite memory constraints and finite execution time can
  cause referentially opaque behavior. In a normal Haskell program, this
  usually leads to a crash or an exception, and the crash or exception
  can itself, in a way, be thought of as being referentially transparent,
  because there is no way for it to both happen and, simultaneously,
  not happen.

  However, in our case we are replicating computations across many
  different pieces of hardware, so there most definitely is a way
  for these aberrant system failures to both happen and not happen
  simultaneously. What happens if the computation of the event runs out
  of memory on one machine, but not on another?

  There exists a strategy for dealing with these problems: if the
  computation of an event experiences a failure on every participant, then
  the event is pushed into the infimum as a failure (i.e. a no-op), but if
  any single participant successfully computes the event then all other
  participants can (somehow) request a "Full Merge" from the successful
  participant. The Full Merge will include the infimum __value__ computed
  by the successful participant, which will include the successful
  application of the problematic event. The error participants can thus
  bypass computation of the problem event altogether, and can simply
  overwrite their infimum with the infimum provided by the Full Merge.

  Doing a full merge can be much more expensive than doing a simple
  'Diff' merge, because it requires transmitting the full value of the
  'EventFold' instead of just the outstanding operations.

  This type represents how computation of the event finished; with either a
  pure result, or some kind of system error.

  TL;DR:

  In general 'SystemError' is probably only ever useful for when your
  event type somehow executes untrusted code (for instance when your event
  type is a Turing-complete DSL that allows users to submit their own
  custom-programmed "events") and you want to limit the resources that
  can be consumed by such untrusted code.  It is much less useful when
  you are encoding some well defined business logic directly in Haskell.
-}
data EventResult e
  = SystemError (Output e)
  | Pure (Output e) (State e)


{- |
  Construct a new 'EventFold' with the given origin and initial
  participant.
-}
new
  :: (Default (State e), Ord p)
  => o {- ^ The "origin", identifying the historical lineage of this CRDT. -}
  -> p {- ^ The initial participant. -}
  -> EventFold o p e
new o participant =
  EventFold
    EventFoldF {
        psOrigin = o,
        psInfimum = Infimum {
            eventId = def,
            participants = Set.singleton participant,
            stateValue = def
          },
        psEvents = mempty
      }


{- |
  Get the outstanding events that need to be propagated to a particular
  participant.
-}
events :: (Ord p) => p -> EventFold o p e -> Diff o p e
events peer (EventFold ef) =
    Diff {
      diffEvents = omitAcknowledged <$> psEvents ef,
      diffOrigin = psOrigin ef,
      diffInfimum = eventId (psInfimum ef)
    }
  where
    {- |
      Don't send the event data to participants which have already
      acknowledged it, saving network and cpu resources.
    -}
    omitAcknowledged (d, acks) =
      (
        case (d, peer `member` acks) of
          (Identity Error {}, _) -> Just (runIdentity d)
          (_, False) -> Just (runIdentity d)
          _ -> Nothing,
        acks
      )


{- | A package containing events that can be merged into an event fold. -}
data Diff o p e = Diff {
     diffEvents :: Map (EventId p) (Maybe (Delta p e), Set p),
     diffOrigin :: o,
    diffInfimum :: EventId p
  }
  deriving stock (Generic)
deriving stock instance (
    Show o, Show p, Show e, Show (Output e)
  ) =>
    Show (Diff o p e)
instance (
    Binary o, Binary p, Binary e, Binary (Output e)
  ) =>
    Binary (Diff o p e)


{- |
  Like 'fullMerge', but merge a remote 'Diff' instead of a full remote
  'EventFold'.
-}
diffMerge
  :: ( Eq (Output e)
     , Eq e
     , Eq o
     , Event e
     , Ord p
     )
  => p {- ^ The "local" participant doing the merge. -}
  -> EventFold o p e {- ^ The local copy of the 'EventFold'. -}
  -> Diff o p e {- ^ The 'Diff' provided by the remote participant. -}
  -> Either
       (MergeError o p e)
       (UpdateResult o p e)

diffMerge
    _
    (EventFold EventFoldF {psOrigin = o1})
    Diff {diffOrigin = o2}
  | o1 /= o2 =
    Left (DifferentOrigins o1 o2)

diffMerge _ ef pak | tooNew =
    Left (DiffTooNew ef pak)
  where
    maxState =
      maximum
      . Set.insert (eventId . psInfimum . unEventFold $ ef)
      . Map.keysSet
      . psEvents
      . unEventFold
      $ ef

    tooNew :: Bool
    tooNew = maxState < diffInfimum pak

diffMerge
    participant
    orig@(EventFold (EventFoldF o infimum d1))
    ep@(Diff d2 _ i2)
  =
    case
      reduce
        i2
        EventFoldF {
          psOrigin = o,
          psInfimum = infimum,
          psEvents =
            Map.Merge.merge
              (Map.Merge.mapMissing (const (first Just)))
              Map.Merge.preserveMissing
              (Map.Merge.zipWithMatched (const mergeAcks))
              (first runIdentity <$> d1)
              d2
        }
    of
      Nothing -> Left (DiffTooSparse orig ep)
      Just (ef1, outputs1) ->
        let (ef2, outputs2) = acknowledge participant ef1
        in
          Right (
            UpdateResult
              (EventFold ef2)
              (Map.union outputs1 outputs2)
              (
                i2 /= eventId infimum
                || not (Map.null d2)
                || ef2 /= unEventFold orig
              )
          )
  where
    mergeAcks :: (Ord p)
      => (Delta p e, Set p)
      -> (Maybe (Delta p e), Set p)
      -> (Maybe (Delta p e), Set p)
    mergeAcks
        (Error output eacks1, acks1)
        (Just (Error _ eacks2), acks2)
      =
        (Just (Error output (eacks1 `union` eacks2)), acks1 `union` acks2)
    mergeAcks
        (Error {}, acks1)
        (d, acks2)
      =
        (d, acks1 `union` acks2)
    mergeAcks
        (d, acks1)
        (Just _, acks2)
      =
        (Just d, acks1 `union` acks2)
    mergeAcks
        (d, acks1)
        (Nothing, acks2)
      =
        (Just d, acks1 `union` acks2)


{- |
  Monotonically merge the information in two 'EventFold's.  The resulting
  'EventFold' may have a higher infimum value, but it will never have a
  lower one (where "higher" and "lower" are measured by 'infimumId' value,
  not the value of the underlying data structure). Only 'EventFold's
  that originated from the same 'new' call can be merged. If the origins
  are mismatched, or if there is some other programming error detected,
  then an error will be returned.

  Returns the new 'EventFold' value, along with the output for all of
  the events that can now be considered "fully consistent".
-}
fullMerge
  :: ( Eq (Output e)
     , Eq e
     , Eq o
     , Event e
     , Ord p
     )
  => p {- ^ The "local" participant doing the merge. -}
  -> EventFold o p e {- ^ The local copy of the 'EventFold'. -}
  -> EventFold o p e {- ^ The remote copy of the 'Eventfold'. -}
  -> Either (MergeError o p e) (UpdateResult o p e)
fullMerge participant (EventFold left) (EventFold right@(EventFoldF o2 i2 d2)) =
  case
    diffMerge
      participant
      (
        EventFold
          left {
            psInfimum = max (psInfimum left) i2
          }
      )
      Diff {
        diffOrigin = o2,
        diffEvents = first (Just . runIdentity) <$> d2,
        diffInfimum = eventId i2
      }
  of
    Left err -> Left err
    Right (UpdateResult ef outputs _prop) ->
      let (ef2, outputs2) = acknowledge participant (unEventFold ef)
      in
        Right (
          UpdateResult
            (EventFold ef2)
            (Map.union outputs outputs2)
            (ef2 /= left || ef2 /= right)
        )


{- |
  The result updating the 'EventFold', which contains:

  - The new 'EventFold' value,
  - The outputs of events that have reached the infimum as a result of
    the update (i.e. "totally consistent outputs"),
  - And a flag indicating whether the other participants need to hear
    about the changes.
-}
data UpdateResult o p e =
    UpdateResult {
             urEventFold :: EventFold o p e,
                            {- ^ The new 'EventFold' value -}
               urOutputs :: Map (EventId p) (Output e),
                            {- ^
                              Any consistent outputs resulting from
                              the update.
                            -}
      urNeedsPropagation :: Bool
                            {- ^
                              'True' if any new information was added to
                              the 'EventFold' which might need propagating
                              to other participants.
                            -}
    }


{- |
  Record the fact that the participant acknowledges the information
  contained in the 'EventFold'. The implication is that the participant
  __must__ base all future operations on the result of this function.

  Returns the new 'EventFold' value, along with the output for all of
  the events that can now be considered "fully consistent".
-}
acknowledge :: (Event e, Ord p)
  => p
  -> EventFoldF o p e Identity
  -> (EventFoldF o p e Identity, Map (EventId p) (Output e))
acknowledge p ef =
    {-
      First do a normal reduction, then do a special acknowledgement of the
      reduction error, if any.
    -}
    let
      (ps2, outputs) =
        runIdentity $
          reduce
            (eventId (psInfimum ef))
            ef {psEvents = fmap ackOne (psEvents ef)}
      (ps3, outputs2) = ackErr p ps2
    in
      (ps3, outputs <> outputs2)
  where
    ackOne (e, acks) = (e, Set.insert p acks)


{- | Acknowledge the reduction error, if one exists. -}
ackErr :: (Event e, Ord p)
  => p
  -> EventFoldF o p e Identity
  -> (EventFoldF o p e Identity, Map (EventId p) (Output e))
ackErr p ef =
  runIdentity $
    reduce
      (eventId (psInfimum ef))
      ef {
        psEvents =
          case Map.minViewWithKey (psEvents ef) of
            Just ((eid, (Identity (Error o eacks), acks)), deltas) ->
              Map.insert
                eid
                (Identity (Error o (Set.insert p eacks)), acks)
                deltas
            _ -> psEvents ef
      }


{- |
  Allow a participant to join in the distributed nature of the
  'EventFold'. Return the 'EventId' at which the participation is
  recorded, and the resulting 'EventFold'. The purpose of returning the
  'EventId' is so that you can use it to tell when the participation
  event has reached the infimum. See also: 'infimumId'
-}
participate :: forall o p e. (Ord p, Event e)
  => p {- ^ The local participant. -}
  -> p {- ^ The participant being added. -}
  -> EventFold o p e
  -> (EventId p, UpdateResult o p e)
participate self peer (EventFold ef) =
    (
      eid,
      let
        (ef2, outputs1) =
          acknowledge
            self
            ef {
              psEvents =
                Map.insert
                  eid
                  (Identity (Join peer), mempty)
                  (psEvents ef)
            }
        (ef3, outputs2) = acknowledge peer ef2
      in
        UpdateResult {
          urEventFold = EventFold ef3,
          urOutputs = Map.union outputs1 outputs2,
          {- 
            By definition, we have added some new information that
            needs propagating.
          -}
          urNeedsPropagation = True
        }
    )
  where
    eid :: EventId p
    eid = nextId self ef


{- |
  Indicate that a participant is removing itself from participating in
  the distributed 'EventFold'.
-}
disassociate :: forall o p e. (Event e, Ord p)
  => p {- ^ The peer removing itself from participation. -}
  -> EventFold o p e
  -> (EventId p, UpdateResult o p e)
disassociate peer (EventFold ef) =
    let
      (ef2, outputs) =
        acknowledge
          peer
          ef {
            psEvents =
              Map.insert
                eid
                (Identity (UnJoin peer), mempty)
                (psEvents ef)
          }
    in
      (
        eid,
        UpdateResult {
          urEventFold = EventFold ef2,
          urOutputs = outputs,
          {- 
            By definition, we have added some new information that
            needs propagating.
          -}
          urNeedsPropagation = True
        }
      )
  where
    eid :: EventId p
    eid = nextId peer ef


{- |
  Introduce a change to the EventFold on behalf of the participant.
  Return the new 'EventFold', along with the projected output of the
  event, along with an 'EventId' which can be used to get the fully
  consistent event output at a later time.
-}
event :: (Ord p, Event e)
  => p
  -> e
  -> EventFold o p e
  -> (Output e, EventId p, UpdateResult o p e)
event p e ef =
  let
    eid = nextId p (unEventFold ef)
  in
    (
      case apply e (projectedValue ef) of
        Pure output _ -> output
        SystemError output -> output,
      eid,
      let
        (ef2, outputs) =
          acknowledge
            p
            (
              (unEventFold ef) {
                psEvents =
                  Map.insert
                    eid
                    (Identity (Event e), mempty)
                    (psEvents (unEventFold ef))
              }
            )
      in
        UpdateResult {
          urEventFold = EventFold ef2,
          urOutputs = outputs,
          urNeedsPropagation =
            {-
              An event is, by definition, adding information to the 'EventFold',
              and the only time we might not need to propagate this this
              information is if the local participant is the only participant.
            -}
            allParticipants (EventFold ef2) /= Set.singleton p
        }
    )


{- | Return the current projected value of the 'EventFold'. -}
projectedValue :: (Event e) => EventFold o p e -> State e
projectedValue
    (
      EventFold
        EventFoldF {
          psInfimum = Infimum {stateValue},
          psEvents
        }
    )
  =
    foldr
      (\ e s ->
        case apply e s of
          Pure _ newState -> newState
          SystemError _ -> s
      )
      stateValue
      changes
  where
    changes = foldMap getDelta (toDescList psEvents)
    getDelta :: (EventId p, (Identity (Delta p e), Set p)) -> [e]
    getDelta (_, (Identity (Event e), _)) = [e]
    getDelta _ = mempty


{- | Return the current infimum value of the 'EventFold'. -}
infimumValue :: EventFold o p e -> State e
infimumValue (EventFold EventFoldF {psInfimum = Infimum {stateValue}}) =
  stateValue


{- | Return the 'EventId' of the infimum value. -}
infimumId :: EventFold o p e -> EventId p
infimumId = eventId . psInfimum . unEventFold


{- |
  Gets the known participants at the infimum.
-}
infimumParticipants :: EventFold o p e -> Set p
infimumParticipants
    (
      EventFold
        EventFoldF {
          psInfimum = Infimum {participants}
        }
    )
  =
    participants


{- |
  Get all known participants. This includes participants that are
  projected for removal.
-}
allParticipants :: (Ord p) => EventFold o p e -> Set p
allParticipants
    (
      EventFold
        EventFoldF {
          psInfimum = Infimum {participants},
          psEvents
        }
    )
  =
    foldr updateParticipants participants (toDescList psEvents)
  where
    updateParticipants :: (Ord p)
      => (EventId p, (Identity (Delta p e), Set p))
      -> Set p
      -> Set p
    updateParticipants (_, (Identity (Join p), _)) = Set.insert p
    updateParticipants _ = id


{- |
  Get all the projected participants. This does not include participants that
  are projected for removal.
-}
projParticipants :: (Ord p) => EventFold o p e -> Set p
projParticipants
    (
      EventFold
        EventFoldF {
          psInfimum = Infimum {participants},
          psEvents
        }
    )
  =
    foldr updateParticipants participants (toDescList psEvents)
  where
    updateParticipants :: (Ord p)
      => (EventId p, (Identity (Delta p e), Set p))
      -> Set p
      -> Set p
    updateParticipants (_, (Identity (Join p), _)) = Set.insert p
    updateParticipants (_, (Identity (UnJoin p), _)) = Set.delete p
    updateParticipants _ = id


{- |
  Returns the participants that we think might be diverging. In
  this context, a participant is "diverging" if there is an event
  that the participant has not acknowledged but we are expecting it
  to acknowledge. Along with the participant, return the last known
  `EventId` which that participant has acknowledged.
-}
divergent :: forall o p e. (Ord p) => EventFold o p e -> Map p (EventId p)
divergent
    (
      EventFold
        EventFoldF {
          psInfimum = Infimum {participants, eventId},
          psEvents
        }
    )
  =
    let (byParticipant, maxEid) = eidByParticipant
    in Map.filter (< maxEid) byParticipant

  where
    eidByParticipant :: (Map p (EventId p), EventId p)
    eidByParticipant =
      foldr
        accum
        (Map.fromList [(p, eventId) | p <- Set.toList participants], eventId)
        (
          let flatten (a, (Identity b, c)) = (a, b, c)
          in (flatten <$> toAscList psEvents)
        )

    accum
      :: (EventId p, Delta p e, Set p)
      -> (Map p (EventId p), EventId p)
      -> (Map p (EventId p), EventId p)

    accum (eid, Join p, acks) (acc, maxEid) =
      (
        unionWith
          max
          (Map.insert p eid acc)
          (Map.fromList [(a, eid) | a <- Set.toList acks]),
        max maxEid eid
      )

    accum (eid, _, acks) (acc, maxEid) =
      (
        unionWith
          max
          acc
          (Map.fromList [(a, eid) | a <- Set.toList acks]),
        max maxEid eid
      )


{- | Return the origin value of the 'EventFold'. -}
origin :: EventFold o p e -> o
origin = psOrigin . unEventFold


{- |
  This helper function is responsible for figuring out if the 'EventFold'
  has enough information to derive a new infimum value. In other words,
  this is where garbage collection happens.
-}
reduce
  :: forall o p e f.
     ( Event e
     , Monad f
     , Ord p
     )
  => EventId p
     {- ^
       The infimum 'EventId' as known by some node in the cluster. "Some
       node" can be different than "this node" in the case where another
       node advanced the infimum before we did (because it knew about our
       acknowledgement, but we didn't know about its acknowledgement)
       and sent us an 'Diff' with this value of the infimum. In this
       case, this infimum value acts as a universal acknowledgement of
       all events coming before it.
     -}
  -> EventFoldF o p e f
  -> f (EventFoldF o p e Identity, Map (EventId p) (Output e))
reduce
    infState
    ef@EventFoldF {
      psInfimum = infimum@Infimum {participants, stateValue},
      psEvents
    }
  =
    case Map.minViewWithKey psEvents of
      Nothing ->
        pure
          (
            EventFoldF {
              psOrigin = psOrigin ef,
              psInfimum = psInfimum ef,
              psEvents = mempty
            },
            mempty
          )
      Just ((eid, (getUpdate, acks)), newDeltas)
        | eid <= eventId infimum -> {- The event is obsolete. Ignore it. -}
            reduce infState ef {
              psEvents = newDeltas
            }
        | isRenegade eid -> {- This is a renegade event. Ignore it. -}
            reduce infState ef {
              psEvents = newDeltas
            }
        | otherwise -> do
            implicitAcks <- unjoins eid

            update <- getUpdate
            let
              {- |
                Join events must be acknowledged by the joining
                participant before moving into the infimum.
              -}
              joining =
                case update of
                  Join p -> Set.singleton p
                  _ -> mempty
            if
                Set.null (((participants `union` joining) \\ acks) \\ implicitAcks)
                || eid <= infState
              then
                case update of
                  Join p ->
                    reduce infState ef {
                      psInfimum = infimum {
                          eventId = eid,
                          participants = Set.insert p participants
                        },
                      psEvents = newDeltas
                    }
                  UnJoin p ->
                    reduce infState ef {
                      psInfimum = infimum {
                          eventId = eid,
                          participants = Set.delete p participants
                        },
                      psEvents = newDeltas
                    }
                  Error output eacks
                    | Set.null (participants \\ eacks) -> do
                        (ps2, outputs) <-
                          reduce infState ef {
                            psInfimum = infimum {
                              eventId = eid
                            }
                          }
                        pure (ps2, Map.insert eid output outputs)
                    | otherwise -> do
                        events_ <- runEvents psEvents
                        pure
                          (
                            EventFoldF {
                              psOrigin = psOrigin ef,
                              psInfimum = psInfimum ef,
                              psEvents = events_
                            },
                            mempty
                          )
                  Event e ->
                    case apply e stateValue of
                      SystemError output -> do
                        events_ <- runEvents newDeltas
                        pure
                          (
                            EventFoldF {
                              psOrigin = psOrigin ef,
                              psInfimum = infimum,
                              psEvents =
                                Map.insert
                                  eid
                                  (Identity (Error output mempty), acks)
                                  events_
                            },
                            mempty
                          )
                      Pure output newState -> do
                        (ps2, outputs) <-
                          reduce infState ef {
                            psInfimum = infimum {
                                eventId = eid,
                                stateValue = newState
                              },
                            psEvents = newDeltas
                          }
                        pure (ps2, Map.insert eid output outputs)
              else do
                events_ <- runEvents psEvents
                pure
                  (
                    EventFoldF {
                      psOrigin = psOrigin ef,
                      psInfimum = psInfimum ef,
                      psEvents = events_
                    },
                    mempty
                  )
  where
    {- | Unwrap the events from their monad. -}
    runEvents
      :: Map (EventId p) (f (Delta p e), Set p)
      -> f (Map (EventId p) (Identity (Delta p e), Set p))
    runEvents events_ =
      Map.fromList <$> sequence [
        do
          d <- fd
          pure (eid, (Identity d, acks))
        | (eid, (fd, acks)) <- Map.toList events_
      ]

    {- | Figure out which nodes have upcoming unjoins. -}
    unjoins
      :: EventId p
         {- ^
           The even under consideration, unjoins only after which we
           are interested.
         -}
      -> f (Set p)
    unjoins eid =
      Set.fromList
      . Map.elems
      . Map.filterWithKey (\k _ -> eid <= k)
      <$> unjoinMap

    {- | The static map of unjoins. -}
    unjoinMap :: f (Map (EventId p) p)
    unjoinMap =
      Map.fromList . catMaybes <$> sequence [
          update >>= \case
            UnJoin p -> pure (Just (eid, p))
            _ -> pure Nothing
          | (eid, (update, _acks)) <- Map.toList psEvents
        ]

    {- |
      Renegade events are events that originate from a non-participating
      peer.  This might happen in a network partition situation, where
      the cluster ejected a peer that later reappears on the network,
      broadcasting updates.
    -}
    isRenegade BottomEid = False
    isRenegade (Eid _ p) = not (p `member` participants)


{- |
  A utility function that constructs the next `EventId` on behalf of
  a participant.
-}
nextId :: (Ord p) => p -> EventFoldF o p e f -> EventId p
nextId p EventFoldF {psInfimum = Infimum {eventId}, psEvents} =
  case maximum (eventId:keys psEvents) of
    BottomEid -> Eid 0 p
    Eid ord _ -> Eid (succ ord) p


{- |
  Return 'True' if progress on the 'EventFold' is blocked on a
  'SystemError'.

  The implication here is that if the local copy is blocked on a
  'SystemError', it needs to somehow arrange for remote copies to send
  full 'EventFold's, not just 'Diff's. A 'diffMerge' is not sufficient
  to get past the block. Only a 'fullMerge' will suffice.
  
  If your system is not using 'SystemError' or else not using 'Diff's,
  then you don't ever need to worry about this function.
-}
isBlockedOnError :: EventFold o p e -> Bool
isBlockedOnError (EventFold ef) =
  case Map.minView (psEvents ef) of
    Just ((Identity (Error _ _), _), _) -> True
    _ -> False


