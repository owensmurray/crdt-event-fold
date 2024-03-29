{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

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
  fullMerge_,
  UpdateResult(..),
  events,
  diffSize,
  diffMerge,
  diffMerge_,
  MergeError(..),
  acknowledge,

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
  source,

  -- * Underlying Types
  EventFold,
  EventId,
  bottomEid,
  Diff,

) where

import Control.Exception (Exception)
import Data.Aeson (FromJSON(parseJSON), ToJSON(toEncoding, toJSON),
  FromJSONKey, ToJSONKey)
import Data.Bifunctor (Bifunctor(first))
import Data.Binary (Binary(get, put))
import Data.Default.Class (Default(def))
import Data.Map (Map, toAscList, toDescList, unionWith)
import Data.Set ((\\), Set, member, union)
import GHC.Generics (Generic)
import Prelude (Applicative(pure), Bool(False, True), Either(Left, Right),
  Enum(succ), Eq((/=), (==)), Foldable(foldr, maximum), Functor(fmap),
  Maybe(Just, Nothing), Monoid(mempty), Ord((<), (<=), compare, max),
  Semigroup((<>)), ($), (.), (<$>), (||), Int, Num, Show, const, fst,
  id, not, otherwise, snd)
import Type.Reflection (Typeable)
import qualified Data.DoubleWord as DW
import qualified Data.Map as Map
import qualified Data.Map.Merge.Lazy as Map.Merge
import qualified Data.Set as Set

{-# ANN module "HLint: ignore Redundant if" #-}
{-# ANN module "HLint: ignore Use catMaybes" #-}

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
data EventFold o p e = EventFold {
     psOrigin :: o,
    psInfimum :: Infimum (State e) p,
     psEvents :: Map (EventId p) (Delta p e, Set p),
    psUnjoins :: Set (EventId p)
                 {- ^
                   The set of events that perform an unjoin with
                   unjoins that have not reached the infimum. This is
                   an optimization so that 'reduce' doesn't have to
                   recompute this every time.
                 -}
  }
  deriving stock (Generic)
deriving anyclass instance (ToJSON o, ToJSON p, ToJSON e, ToJSON (State e), ToJSON (Output e)) => ToJSON (EventFold o p e)
deriving anyclass instance (Ord p, FromJSON o, FromJSON p, FromJSON e, FromJSON (Output e), FromJSON (State e)) => FromJSON (EventFold o p e)
deriving stock instance
    ( Eq (Output e)
    , Eq o
    , Eq p
    , Eq e
    )
  =>
    Eq (EventFold o p e)
instance
    (
      Binary o,
      Binary p,
      Binary e,
      Binary (State e),
      Binary (Output e)
    )
  =>
    Binary (EventFold o p e)
deriving stock instance
    ( Show (Output e)
    , Show o
    , Show p
    , Show e
    , Show (State e)
    )
  => Show (EventFold o p e)


{- |
  `Infimum` is the infimum, or greatest lower bound, of the possible
  values of @s@.
-}
data Infimum s p = Infimum
  {      eventId :: EventId p
  , participants :: Set p
  ,   stateValue :: s
  }
  deriving stock (Generic, Show)
  deriving anyclass (ToJSON, FromJSON)
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
  deriving anyclass (ToJSON, FromJSON, ToJSONKey, FromJSONKey, Binary)
instance Default (EventId p) where
  def = BottomEid


{- |
  The participant the created an event, if there is one (which there
  isn't for 'bottomEid').
-}
source :: EventId p -> Maybe p
source = \case
  BottomEid -> Nothing
  Eid _ p -> Just p


{- | Newtype around 'DW.Word256' to supply typeclass instances. -}
newtype Word256 = Word256 {
    unWord256 :: DW.Word256
  }
  deriving stock (Generic)
  deriving newtype (Eq, Ord, Show, Enum, Num)
instance FromJSON Word256 where
  parseJSON v = do
    (a, b, c, d) <- parseJSON v
    pure (Word256 (DW.Word256 (DW.Word128 a b) (DW.Word128 c d)))
instance ToJSON Word256 where
  toJSON (Word256 (DW.Word256 (DW.Word128 a b) (DW.Word128 c d))) =
    toJSON (a, b, c, d)
  toEncoding (Word256 (DW.Word256 (DW.Word128 a b) (DW.Word128 c d))) =
    toEncoding (a, b, c, d)
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
  deriving stock (Generic)
deriving anyclass instance (Ord p, FromJSON o, FromJSON p, FromJSON e, FromJSON (State e), FromJSON (Output e)) => FromJSON (MergeError o p e)
deriving anyclass instance (ToJSON o, ToJSON p, ToJSON e, ToJSON (Output e), ToJSON (State e)) => ToJSON (MergeError o p e)
deriving stock instance
    ( Show (Output e)
    , Show o
    , Show p
    , Show e
    , Show (State e)
    )
  =>
    Show (MergeError o p e)
instance (Typeable o, Typeable p, Typeable e, Show (Output e), Show o, Show p, Show e, Show (State e)) => Exception (MergeError o p e)


{- | `Delta` is how we represent mutations to the event fold state. -}
data Delta p e
  = Join p
  | UnJoin p
  | EventD e
  | Error (Output e) (Set p)
  deriving stock (Generic)
deriving anyclass instance (ToJSON p, ToJSON e, ToJSON (Output e)) => ToJSON (Delta p e)
deriving anyclass instance (Ord p, FromJSON p, FromJSON e, FromJSON (Output e)) => (FromJSON (Delta p e))
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
class Event p e where
  type Output e
  type State e
  {- | Apply an event to a state value. **This function MUST be total!!!** -}
  apply :: e -> State e -> EventResult e

  join :: p -> State e -> State e
  join _ s = s

  unjoin :: p -> State e -> State e
  unjoin _ s = s
{- | The most trivial event type. -}
instance Event p () where
  type Output () = ()
  type State () = ()
  apply () () = Pure () ()
{- | The union of two event types. -}
instance (Event p a, Event p b) => Event p (Either a b) where
  type Output (Either a b) = Either (Output a) (Output b)
  type State (Either a b) = (State a, State b)

  apply (Left e) (a, b) =
    case apply @p e a of
      SystemError o -> SystemError (Left o)
      Pure o s -> Pure (Left o) (s, b)
  apply (Right e) (a, b) =
    case apply @p e b of
      SystemError o -> SystemError (Right o)
      Pure o s -> Pure (Right o) (a, s)

  join p (a, b) =
    (join @p @a p a, join @p @b p b)

  unjoin p (a, b) =
    (unjoin @p @a p a, unjoin @p @b p b)


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
  :: forall o p e.
     ( Default (State e)
     , Event p e
     , Ord p
     )
  => o {- ^ The "origin", identifying the historical lineage of this CRDT. -}
  -> p {- ^ The initial participant. -}
  -> EventFold o p e
new o participant =
  EventFold {
    psOrigin = o,
    psInfimum = Infimum {
        eventId = def,
        participants = Set.singleton participant,
        stateValue = join @p @e participant def
      },
    psEvents = mempty,
    psUnjoins = mempty
  }


{- |
  Get the outstanding events that need to be propagated to a particular
  participant.

  It isn't always the case that a less expensive 'diffMerge' is sufficient
  to maintain consistency. For instance, if the initial 'participate'
  for a participant hasn't reached the infimum yet then there is no way
  to guarantee that the target will receive /every/ new event from every
  participant (because an old participant might not even know about the
  new participant, because being part of the infimum is the /definition/
  of all participants knowing a thing).

  If the new participant doesn't receive every event, then it obviously
  can't 'apply' the missing events. Therefore, until it's 'participate'
  event is part of the infimum, it must receive infimum values that have
  the missing events pre-applied by some other participant.
-}
events
  :: forall o p e. (Ord p)
  => p {- ^ The participant to which we are sending the 'Diff'. -}
  -> EventFold o p e {- ^ The EventFold being propagated. -}
  -> Maybe (Diff o p e)
     {- ^
       'Nothing' if the participant must perform a 'fullMerge' in order
       to maintain consistency. 'Just' if a less expensive 'diffMerge'
       will suffice.
     -}
events peer ef =
    if
      diffOk
        (participants (psInfimum ef))
        (snd <$> Map.toAscList (psEvents ef))
    then
      Just
        Diff {
          diffEvents = omitAcknowledged <$> psEvents ef,
          diffOrigin = psOrigin ef,
          diffInfimum = eventId (psInfimum ef),
          diffUnjoins = psUnjoins ef
        }
    else
      Nothing
  where
    {- |
      Return 'True' if it is ok to send a diff. 'False' if a full merge
      must be performed.
    -}
    diffOk :: Set p -> [(Delta p e, Set p)] -> Bool
    diffOk accPeers someEvents =
        if peer `member` accPeers then
          {-
            Even if the target is part of the infimum, we still have
            to make sure the target doesn't have an upcoming 'UnJoin'
            (regardless if it is followed by another 'Join', otherwise
            we would just use 'projParticipants').
          -}
          case someEvents of
            (e, _):more ->
              diffOk (accumulatePeers e) more
            [] -> True
        else
          False
      where
        accumulatePeers :: Delta p e -> Set p
        accumulatePeers = \case
          UnJoin p -> Set.delete p accPeers
          _ -> accPeers

    {- |
      Don't send the event data to participants which have already
      acknowledged it, saving network and cpu resources.
    -}
    omitAcknowledged (d, acks) =
      (
        case (d, peer `member` acks) of
          (Error {}, _) -> Just d
          (_, False) -> Just d
          _ -> Nothing,
        acks
      )


{- | A package containing events that can be merged into an event fold. -}
data Diff o p e = Diff
  {  diffEvents :: Map (EventId p) (Maybe (Delta p e), Set p)
  ,  diffOrigin :: o
  , diffInfimum :: EventId p
  , diffUnjoins :: Set (EventId p)
  }
  deriving stock (Generic)
deriving stock instance (Eq o, Eq p, Eq e, Eq (Output e)) => Eq (Diff o p e)
deriving anyclass instance (ToJSON o, ToJSON p, ToJSON e, ToJSON (Output e)) => ToJSON (Diff o p e)
deriving anyclass instance (Ord p, FromJSON o, FromJSON p, FromJSON e, FromJSON (Output e)) => FromJSON (Diff o p e)
deriving stock instance (
    Show o, Show p, Show e, Show (Output e)
  ) =>
    Show (Diff o p e)
instance (
    Binary o, Binary p, Binary e, Binary (Output e)
  ) =>
    Binary (Diff o p e)


{-|
  Return the number of events contained in the diff. This information
  might be useful for optimizing performance by, for instance, choosing to
  use `diffMerge` instead of `fullMerge` when the diff is small or zero.
-}
diffSize :: Diff o p e -> Int
diffSize Diff { diffEvents } =
  Map.size diffEvents


{- |
  Like 'fullMerge', but merge a remote 'Diff' instead of a full remote
  'EventFold'.
-}
diffMerge
  :: ( Eq (Output e)
     , Eq e
     , Eq o
     , Event p e
     , Ord p
     )
  => p {- ^ The "local" participant doing the merge. -}
  -> EventFold o p e {- ^ The local copy of the 'EventFold'. -}
  -> Diff o p e {- ^ The 'Diff' provided by the remote participant. -}
  -> Either
       (MergeError o p e)
       (UpdateResult o p e)

diffMerge participant orig ep =
  case diffMerge_ orig ep of
    Left err -> Left err
    Right (UpdateResult ef1 outputs1 prop1) ->
      let UpdateResult ef2 outputs2 prop2 = acknowledge participant ef1
      in
        Right (
          UpdateResult
            ef2
            (Map.union outputs1 outputs2)
            (prop1 || prop2)
        )


{- | Like 'diffMerge', but without automatic acknowledgement. -}
diffMerge_
  :: forall o p e.
     ( Eq (Output e)
     , Eq e
     , Eq o
     , Event p e
     , Ord p
     )
  => EventFold o p e {- ^ The local copy of the 'EventFold'. -}
  -> Diff o p e {- ^ The 'Diff' provided by the remote participant. -}
  -> Either
       (MergeError o p e)
       (UpdateResult o p e)

diffMerge_
    (EventFold {psOrigin = o1})
    Diff {diffOrigin = o2}
  | o1 /= o2 =
    Left (DifferentOrigins o1 o2)

diffMerge_ ef pak | tooNew =
    Left (DiffTooNew ef pak)
  where
    maxState =
      maximum
      . Set.insert (eventId . psInfimum $ ef)
      . Map.keysSet
      . psEvents
      $ ef

    tooNew :: Bool
    tooNew = maxState < diffInfimum pak

diffMerge_
    orig@(EventFold o infimum d1 unjoins)
    diff@(Diff d2 _ i2 diffUnjoins)
  =
    let
      mergedEvents :: Maybe (Map (EventId p) (Delta p e, Set p))
      mergedEvents =
        Map.Merge.mergeA
          Map.Merge.preserveMissing
          (Map.Merge.traverseMaybeMissing includeDiffEvents)
          (Map.Merge.zipWithMatched (const mergeAcks))
          d1
          d2
    in
      case mergedEvents of
        Nothing -> Left (DiffTooSparse orig diff)
        Just events_ ->
          let
            (ef, outputs) =
              reduce
                i2
                EventFold {
                  psOrigin = o,
                  psInfimum = infimum,
                  psEvents = events_,
                  psUnjoins = unjoins `Set.union` diffUnjoins
                }
          in
            Right (
              UpdateResult
                ef
                outputs
                (
                  i2 /= eventId infimum
                  || not (Map.null d2)
                  || ef /= orig
                )
            )
  where
    includeDiffEvents :: EventId p -> (Maybe a, b) -> Maybe (Maybe (a, b))
    includeDiffEvents eid (md, acks) =
      {-
        Don't consider diff events that are behind the infimum of the
        EventFold being updated.
      -}
      if eid <= eventId infimum
        then
          {- Don't incude the key, but also don't fail the merge. -}
          Just Nothing
        else
          case md of
            Nothing ->
              {-
                The diff assumes this replica has knowledge of the event,
                but it does not. Fail the merge.
              -}
              Nothing
            Just d ->
              Just $ Just (d, acks)

    mergeAcks
      :: (Delta p e, Set p)
      -> (Maybe (Delta p e), Set p)
      -> (Delta p e, Set p)
    mergeAcks
        (Error output eacks1, acks1)
        (Just (Error _ eacks2), acks2)
      =
        (Error output (eacks1 `union` eacks2), acks1 `union` acks2)
    mergeAcks
        left@(Error {}, acks1)
        (md, acks2)
      =
        case md of
          Nothing -> left
          Just d ->
            (d, acks1 `union` acks2)
    mergeAcks
        (d, acks1)
        (Just _, acks2)
      =
        (d, acks1 `union` acks2)
    mergeAcks
        (d, acks1)
        (Nothing, acks2)
      =
        (d, acks1 `union` acks2)


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
     , Event p e
     , Ord p
     )
  => p {- ^ The "local" participant doing the merge. -}
  -> EventFold o p e {- ^ The local copy of the 'EventFold'. -}
  -> EventFold o p e {- ^ The remote copy of the 'Eventfold'. -}
  -> Either (MergeError o p e) (UpdateResult o p e)
fullMerge participant left right =
  case fullMerge_ left right of
    Left err -> Left err
    Right (UpdateResult ef1 outputs1 _) ->
      let UpdateResult ef2 outputs2 _ = acknowledge participant ef1
      in
        Right (
          UpdateResult
            ef2
            (Map.union outputs1 outputs2)
            (ef2 /= left || ef2 /= right)
        )


{- | Like 'fullMerge', but without the automatic acknowlegement.  -}
fullMerge_
  :: ( Eq (Output e)
     , Eq e
     , Eq o
     , Event p e
     , Ord p
     )
  => EventFold o p e {- ^ The local copy of the 'EventFold'. -}
  -> EventFold o p e {- ^ The remote copy of the 'Eventfold'. -}
  -> Either (MergeError o p e) (UpdateResult o p e)
fullMerge_ left right@(EventFold o2 i2 d2 unjoins) =
  case
    diffMerge_
      left {
        psInfimum = max (psInfimum left) i2
      }
      Diff {
        diffOrigin = o2,
        diffEvents = first Just <$> d2,
        diffInfimum = eventId i2,
        diffUnjoins = unjoins
      }
  of
    Left err -> Left err
    Right (UpdateResult ef outputs _prop) ->
      Right (
        UpdateResult
          ef
          outputs
          (ef /= left || ef /= right)
      )


{- |
  The result updating the 'EventFold', which contains:

  - The new 'EventFold' value,
  - The outputs of events that have reached the infimum as a result of
    the update (i.e. "totally consistent outputs"),
  - And a flag indicating whether the other participants need to hear
    about the changes.
-}
data UpdateResult o p e = UpdateResult
  {        urEventFold :: EventFold o p e
                          {- ^ The new 'EventFold' value -}
  ,          urOutputs :: Map (EventId p) (Output e)
                          {- ^
                            Any consistent outputs resulting from
                            the update.
                          -}
  , urNeedsPropagation :: Bool
                          {- ^
                            'True' if any new information was added to
                            the 'EventFold' which might need propagating
                            to other participants.
                          -}
  }
deriving stock instance
  ( Show (Output e)
  , Show (State e)
  , Show e
  , Show o
  , Show p
  )
  => Show (UpdateResult o p e)


{- |
  Record the fact that the participant acknowledges the information
  contained in the 'EventFold'. The implication is that the participant
  __must__ base all future operations on the result of this function.

  Returns the new 'EventFold' value, along with the output for all of
  the events that can now be considered "fully consistent".
-}
acknowledge
  :: ( Eq (Output e)
     , Eq e
     , Eq o
     , Event p e
     , Ord p
     )
  => p
  -> EventFold o p e
  -> UpdateResult o p e
acknowledge p ef =
  let (ef2, outputs) = acknowledge_ p ef
  in
    UpdateResult {
      urEventFold = ef2,
      urOutputs = outputs,
      urNeedsPropagation = ef /= ef2
    }


{- | Internal version of 'acknowledge'. -}
acknowledge_
  :: ( Event p e
     , Ord p
     )
  => p
  -> EventFold o p e
  -> (EventFold o p e, Map (EventId p) (Output e))
acknowledge_ p ef =
    {-
      First do a normal reduction, then do a special acknowledgement of the
      reduction error, if any.
    -}
    let
      (ps2, outputs) =
        reduce
          (eventId (psInfimum ef))
          ef {psEvents = fmap ackOne (psEvents ef)}
      (ps3, outputs2) = ackErr p ps2
    in
      (ps3, outputs <> outputs2)
  where
    ackOne (e, acks) = (e, Set.insert p acks)


{- | Acknowledge the reduction error, if one exists. -}
ackErr
  :: ( Event p e
     , Ord p
     )
  => p
  -> EventFold o p e
  -> (EventFold o p e, Map (EventId p) (Output e))
ackErr p ef =
  case Map.minViewWithKey (psEvents ef) of
    Just ((eid, (Error o eacks, acks)), deltas) ->
      reduce
        (eventId (psInfimum ef))
        ef {
          psEvents =
            Map.insert
              eid
              (Error o (Set.insert p eacks), acks)
              deltas
        }
    _ -> (ef, mempty)


{- |
  Allow a participant to join in the distributed nature of the
  'EventFold'. Return the 'EventId' at which the participation is
  recorded, and the resulting 'EventFold'. The purpose of returning the
  'EventId' is so that you can use it to tell when the participation
  event has reached the infimum. See also: 'infimumId'
-}
participate
  :: forall o p e.
     ( Event p e
     , Ord p
     )
  => p {- ^ The local participant. -}
  -> p {- ^ The participant being added. -}
  -> EventFold o p e
  -> (EventId p, UpdateResult o p e)
participate self peer ef =
    (
      eid,
      let
        (ef2, outputs) =
          acknowledge_
            self
            ef {
              psEvents =
                Map.insert
                  eid
                  (Join peer, mempty)
                  (psEvents ef)
            }
      in
        UpdateResult {
          urEventFold = ef2,
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
    eid = nextId self ef


{- |
  Indicate that a participant is removing itself from participating in
  the distributed 'EventFold'.
-}
disassociate
  :: forall o p e.
     ( Event p e
     , Ord p
     )
  => p {- ^ The peer removing itself from participation. -}
  -> EventFold o p e
  -> (EventId p, UpdateResult o p e)
disassociate peer ef =
    let
      (ef2, outputs) =
        acknowledge_
          peer
          ef {
            psEvents =
              Map.insert
                eid
                (UnJoin peer, mempty)
                (psEvents ef),
            psUnjoins = Set.insert eid (psUnjoins ef)
          }
    in
      (
        eid,
        UpdateResult {
          urEventFold = ef2,
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
event
  :: forall o p e.
     ( Event p e
     , Ord p
     )
  => p
  -> e
  -> EventFold o p e
  -> (Output e, EventId p, UpdateResult o p e)
event p e ef =
  let
    eid = nextId p ef
  in
    (
      case apply @p e (projectedValue ef) of
        Pure output _ -> output
        SystemError output -> output,
      eid,
      let
        (ef2, outputs) =
          reduce
            (eventId (psInfimum ef))
            (
              ef {
                psEvents =
                  Map.insert
                    eid
                    (EventD e, Set.singleton p)
                    (psEvents ef)
              }
            )
      in
        UpdateResult {
          urEventFold = ef2,
          urOutputs = outputs,
          urNeedsPropagation =
            {-
              An event is, by definition, adding information to the 'EventFold',
              and the only time we might not need to propagate this this
              information is if the local participant is the only participant.
            -}
            allParticipants ef2 /= Set.singleton p
        }
    )


{- | Return the current projected value of the 'EventFold'. -}
projectedValue :: forall o p e. (Event p e) => EventFold o p e -> State e
projectedValue
    EventFold {
      psInfimum = Infimum {stateValue},
      psEvents
    }
  =
    foldr
      applyDelta
      stateValue
      changes
  where
    applyDelta :: Delta p e -> State e -> State e
    applyDelta d s =
      case d of
        Join p -> join @p @e p s
        UnJoin p -> unjoin @p @e p s
        EventD e ->
          case apply @p e s of
            Pure _ newState -> newState
            SystemError _ -> s
        Error{} -> s

    changes :: [Delta p e]
    changes = fst . snd <$> toDescList psEvents


{- | Return the current infimum value of the 'EventFold'. -}
infimumValue :: EventFold o p e -> State e
infimumValue EventFold {psInfimum = Infimum {stateValue}} =
  stateValue


{- | Return the 'EventId' of the infimum value. -}
infimumId :: EventFold o p e -> EventId p
infimumId = eventId . psInfimum


{- | Gets the known participants at the infimum. -}
infimumParticipants :: EventFold o p e -> Set p
infimumParticipants
    EventFold {
      psInfimum = Infimum {participants}
    }
  =
    participants


{- |
  Get all known participants. This includes participants that are
  projected for removal.
-}
allParticipants :: (Ord p) => EventFold o p e -> Set p
allParticipants
    EventFold {
      psInfimum = Infimum {participants},
      psEvents
    }
  =
    foldr updateParticipants participants (toDescList psEvents)
  where
    updateParticipants :: (Ord p)
      => (EventId p, (Delta p e, Set p))
      -> Set p
      -> Set p
    updateParticipants (_, (Join p, _)) = Set.insert p
    updateParticipants _ = id


{- |
  Get all the projected participants. This does not include participants that
  are projected for removal.
-}
projParticipants :: (Ord p) => EventFold o p e -> Set p
projParticipants
    EventFold {
      psInfimum = Infimum {participants},
      psEvents
    }
  =
    foldr updateParticipants participants (toDescList psEvents)
  where
    updateParticipants :: (Ord p)
      => (EventId p, (Delta p e, Set p))
      -> Set p
      -> Set p
    updateParticipants (_, (Join p, _)) = Set.insert p
    updateParticipants (_, (UnJoin p, _)) = Set.delete p
    updateParticipants _ = id


{- |
  Returns the participants that we think might be diverging. In
  this context, a participant is "diverging" if there is an event
  that the participant has not acknowledged but we are expecting it
  to acknowledge. Along with the participant, return the last known
  `EventId` which that participant has acknowledged, or 'BottomEid'
  if the participant has a acknowledged no events, as may be the case
  immediately after the participant joined replication.
-}
divergent :: forall o p e. (Ord p) => EventFold o p e -> Map p (EventId p)
divergent
    EventFold {
      psInfimum = Infimum {participants, eventId},
      psEvents
    }
  =
    let (byParticipant, maxEid) = eidByParticipant
    in Map.filter (< maxEid) byParticipant

  where
    eidByParticipant :: (Map p (EventId p), EventId p)
    eidByParticipant =
      foldr
        accum
        (
          Map.fromList [(p, eventId) | p <- Set.toList participants],
          eventId
        )
        (
          let flatten (a, (b, c)) = (a, b, c)
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
          (Map.insert p BottomEid acc)
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
origin = psOrigin


{- |
  This helper function is responsible for figuring out if the 'EventFold'
  has enough information to derive a new infimum value. In other words,
  this is where garbage collection happens.
-}
reduce
  :: forall o p e.
     ( Event p e
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
  -> EventFold o p e
  -> (EventFold o p e, Map (EventId p) (Output e))
reduce
    infState
  =
    go
  where
    go
      :: EventFold o p e
      -> (EventFold o p e, Map (EventId p) (Output e))
    go
        ef@EventFold
          { psInfimum = infimum@Infimum {participants, stateValue}
          , psEvents
          , psUnjoins
          }
      =
        case Map.minViewWithKey psEvents of
          Nothing ->
            (
              EventFold {
                psOrigin = psOrigin ef,
                psInfimum = psInfimum ef,
                psEvents = mempty,
                psUnjoins = mempty
              },
              mempty
            )
          Just ((eid, (update, acks)), newDeltas)
            | eid <= eventId infimum -> {- The event is obsolete. Ignore it. -}
                go ef {
                  psEvents = newDeltas,
                  psUnjoins = dropObsoleteUnjoins eid psUnjoins
                }
            | isRenegade eid ef -> {- This is a renegade event. Ignore it. -}
                go ef {
                  psEvents = newDeltas,
                  psUnjoins = dropObsoleteUnjoins eid psUnjoins
                }
            | otherwise ->
                let
                  implicitAcks =
                    Set.fromList
                      [ p | Just p <- source <$> Set.toList psUnjoins ]
                  {- |
                    Join events must be acknowledged by the joining
                    participant before moving into the infimum.
                  -}
                  joining =
                    case update of
                      Join p -> Set.singleton p
                      _ -> mempty
                in
                  if
                      Set.null (((participants `union` joining) \\ acks) \\ implicitAcks)
                      || eid <= infState
                    then
                      {-
                        This branch means to roll the update into the
                        infimum. The @else@ branch means we do not.
                      -}
                      case update of
                        Join p ->
                          go ef {
                            psInfimum = infimum {
                                eventId = eid,
                                participants = Set.insert p participants,
                                stateValue = join @p @e p stateValue
                              },
                            psEvents = newDeltas,
                            psUnjoins = dropObsoleteUnjoins eid psUnjoins
                          }
                        UnJoin p ->
                          go ef {
                            psInfimum = infimum {
                                eventId = eid,
                                participants = Set.delete p participants,
                                stateValue = unjoin @p @e p stateValue
                              },
                            psEvents = newDeltas,
                            psUnjoins = dropObsoleteUnjoins eid psUnjoins
                          }
                        Error output eacks
                          | Set.null (participants \\ eacks) ->
                              let
                                (ps2, outputs) =
                                  go ef {
                                    psInfimum = infimum {
                                      eventId = eid
                                    },
                                    psUnjoins = dropObsoleteUnjoins eid psUnjoins
                                  }
                              in
                                (ps2, Map.insert eid output outputs)
                          | otherwise ->
                              (
                                EventFold {
                                  psOrigin = psOrigin ef,
                                  psInfimum = psInfimum ef,
                                  psEvents,
                                  psUnjoins
                                },
                                mempty
                              )
                        EventD e ->
                          case apply @p e stateValue of
                            SystemError output ->
                              (
                                EventFold {
                                  psOrigin = psOrigin ef,
                                  psInfimum = infimum,
                                  psEvents =
                                    Map.insert
                                      eid
                                      (Error output mempty, acks)
                                      newDeltas,
                                  psUnjoins = dropObsoleteUnjoins eid psUnjoins
                                },
                                mempty
                              )
                            Pure output newState ->
                              let
                                (ps2, outputs) =
                                  go ef {
                                    psInfimum = infimum {
                                        eventId = eid,
                                        stateValue = newState
                                      },
                                    psEvents = newDeltas,
                                    psUnjoins = dropObsoleteUnjoins eid psUnjoins
                                  }
                              in
                                (ps2, Map.insert eid output outputs)
                    else
                      (
                        EventFold {
                          psOrigin = psOrigin ef,
                          psInfimum = psInfimum ef,
                          psEvents,
                          psUnjoins
                        },
                        mempty
                      )

    dropObsoleteUnjoins :: EventId p -> Set (EventId p) -> Set (EventId p)
    dropObsoleteUnjoins newInfimumEid unjoins =
      let (_, gt) = Set.split newInfimumEid unjoins
      in gt

    {- |
      Renegade events are events that originate from a non-participating
      peer.  This might happen in a network partition situation, where
      the cluster ejected a peer that later reappears on the network,
      broadcasting updates.
    -}
    isRenegade :: EventId p -> EventFold o p e -> Bool
    isRenegade BottomEid _ = False
    isRenegade (Eid _ p) ef =
      not (p `member` participants (psInfimum ef))


{- |
  A utility function that constructs the next `EventId` on behalf of
  a participant.
-}
nextId
  :: forall o p e.
     p
  -> EventFold o p e
  -> EventId p
nextId p EventFold {psInfimum = Infimum {eventId}, psEvents} =
  let
    maxEid :: EventId p
    maxEid =
      case Map.maxViewWithKey psEvents of
        Just ((eid, _), _) -> eid
        Nothing -> eventId
  in
    case maxEid of
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
isBlockedOnError ef =
  case Map.minView (psEvents ef) of
    Just ((Error _ _, _), _) -> True
    _ -> False


{- | The bottom 'EventId', possibly useful for comparison in tests. -}
bottomEid :: EventId p
bottomEid = BottomEid


