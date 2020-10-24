
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- |
  Description: Garbage collected event folding CRDT.

  This module provides a CRDT data structure that collects and applies
  operations (called "events") that mutate an underlying data structure
  (like folding).

  In addition to mutating the underlying data, each operation can also
  produce an output that can be obtained by the client. The output can be
  either totally consistent across all replicas (which is slower), or it
  can be returned immediately and possibly reflect an inconsistent state.
-}
module Data.CRDT.EventFold (
  EventFoldF,
  EventFold,
  Event(..),
  EventResult(..),
  StateId,
  MergeError(..),
  EventPack,

  new,
  event,
  mergeMaybe,
  mergeEither,
  acknowledge,
  fullMerge,

  participate,
  disassociate,

  events,
  isBlockedOnError,
  projectedValue,
  infimumValue,
  infimumId,
  infimumParticipants,
  allParticipants,
  projParticipants,
  divergent,
  origin,
) where


import Data.Aeson (ToJSONKeyFunction(ToJSONKeyText), ToJSON, ToJSONKey,
  defaultOptions, encode, genericToEncoding, genericToJSON, toEncoding,
  toJSON, toJSONKey)
import Data.Aeson.Encoding (text)
import Data.Aeson.Types (Options, camelTo2, constructorTagModifier,
  fieldLabelModifier)
import Data.Bifunctor (first)
import Data.Binary (Binary(get, put))
import Data.Char (isUpper)
import Data.Default.Class (Default(def))
import Data.DoubleWord (Word128(Word128), Word256(Word256))
import Data.Functor.Identity (Identity(Identity), runIdentity)
import Data.Map (Map, keys, toAscList, toDescList, unionWith)
import Data.Maybe (catMaybes)
import Data.Set ((\\), Set, member, union)
import Data.String (IsString, fromString)
import Data.Word (Word64)
import GHC.Generics (Generic)
import qualified Data.ByteString.Lazy.Char8 as BSL8
import qualified Data.Map as Map
import qualified Data.Map.Merge.Lazy as Map.Merge
import qualified Data.Set as Set


{- |
  This represents a replicated data structure into which participants can
  add 'Event's that are folded into a base 'State'. You can also think
  of the "events" as operations that mutate the base state, and the point
  of this CRDT is to coordinate the application of the operations across
  all participants so that they are applied consistently even if the
  operations themselves are not commutative, idempotent, or monotonic.
  Those properties to the CRDT by the way in which it manages the events,
  and it is therefore unnecessary that the events themselves have them.

  Variables are:

  - @o@ - Origin
  - @p@ - Participant
  - @e@ - Event
  - @f@ - The Monad in which the events live

  The "Origin" is a value that is more or less meant to identify the
  "thing" being replicated, and in particular identify the historical
  lineage of the 'EventFold'. The idea is that it is meaningless to
  try and merge two 'EventFold's that do not share a common history
  (identified by the origin value) and doing so is a programming error. It
  is only used to try and check for this type of programming error and
  throw an exception if it happens instead of producing undefined (and
  difficult to detect) behavior.
-}
data EventFoldF o p e f = EventFold {
     psOrigin :: o,
    psInfimum :: Infimum (State e) p,
     psEvents :: Map (StateId p) (f (Delta p e), Set p)
  } deriving (Generic)
deriving instance
    (
      Eq (f (Delta p e)),
      Eq o,
      Eq p,
      Eq e,
      Eq (Output e)
    )
  =>
    Eq (EventFoldF o p e f)
instance
    (
      ToJSON (f (Delta p e)),
      Show p,
      ToJSON o,
      ToJSON p,
      ToJSON e,
      ToJSON (State e),
      ToJSON (Output e)
    )
  =>
    Show (EventFoldF o p e f)
  where
    show = BSL8.unpack . encode
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
instance
    (
      ToJSON (f (Delta p e)),
      ToJSON o,
      ToJSON p,
      ToJSON e,
      ToJSON (State e),
      ToJSON (Output e),
      Show p
    )
  =>
    ToJSON (EventFoldF o p e f)
  where
    toJSON = genericToJSON prefixedLispCase
    toEncoding = genericToEncoding prefixedLispCase


type EventFold o p e = EventFoldF o p e Identity


{- |
  `Infimum` is the infimum, or greatest lower bound, of the possible
  values of @s@.
-}
data Infimum s p = Infimum {
         stateId :: StateId p,
    participants :: Set p,
      stateValue :: s
  } deriving (Generic, Show)
instance (Binary s, Binary p) => Binary (Infimum s p)
instance (Eq p) => Eq (Infimum s p) where
  Infimum s1 _ _ == Infimum s2 _ _ = s1 == s2
instance (Ord p) => Ord (Infimum s p) where
  compare (Infimum s1 _ _) (Infimum s2 _ _) = compare s1 s2
instance (Show p, ToJSON s, ToJSON p) => ToJSON (Infimum s p) where
  toJSON = genericToJSON lispCase
  toEncoding = genericToEncoding lispCase


{- |
  `StateId` is a monotonically increasing, totally ordered identification
  value which allows us to lend the attribute of monotonicity to state
  operations which would not naturally be monotonic.
-}
data StateId p
  = BottomSid
  | Sid Word256 p
  deriving (Generic, Eq, Ord, Show)
instance (Show p) => ToJSON (StateId p) where
  toJSON = toJSON . show
instance (Show p) => ToJSONKey (StateId p) where
  toJSONKey = ToJSONKeyText showt (text . showt)
instance (Binary p) => Binary (StateId p) where
  put = put . toMaybe
    where
      toMaybe :: StateId p -> Maybe (Word64, Word64, Word64, Word64, p)
      toMaybe BottomSid =
        Nothing
      toMaybe (Sid (Word256 (Word128 a b) (Word128 c d)) p) =
        Just (a, b, c, d, p)
  get = do
    theThing <- get
    return $ case theThing of
      Nothing -> BottomSid
      Just (a, b, c, d, p) -> Sid (Word256 (Word128 a b) (Word128 c d)) p
instance Default (StateId p) where
  def = BottomSid


{- |
  This is the exception type for illegal merges. These errors indicate
  a serious violation of the contract, and probably indicate serious bugs.
-}
data MergeError o p e
  = DifferentOrigins o o
    {- ^
      The 'EventFold's do not have the same origin. It makes no sense
      to merge 'EventFold's that have different origins because they
      do not share a common history.
    -}
  | EventPackTooNew (EventFold o p e) (EventPack o p e)
    {- ^
      The 'EventPack''s infimum is greater than any event known to
      'EventFold' into which it is being merged. This should be
      impossible and indicates that either the local 'EventFold' has
      rolled back an event that it had previously acknowledged, or else
      the source of the 'EventPack' moved the infimum forward without
      a full acknowledgement from all peers. Both of these conditions
      should be regarded as serious bugs.
    -}
  | EventPackTooSparse (EventFold o p e) (EventPack o p e)
    {- ^
      The 'EventPack' assumes we know about events that we do not in
      fact know about. This is only possible if we rolled back our
      copy of the state somehow and "forgot" about state that we had
      previous acknowledged, or else some other participant erroneously
      acknowledged some events on our behalf.
    -}
deriving instance
    ( Show (Output e)
    , Show e
    , Show o
    , Show p
    , ToJSON (Output e)
    , ToJSON (State e)
    , ToJSON e
    , ToJSON o
    , ToJSON p
    )
  =>
    Show (MergeError o p e)


{- | `Delta` is how we represent mutations to the event fold state. -}
data Delta p e
  = Join p
  | UnJoin p
  | Event e
  | Error (Output e) (Set p)
  deriving (Generic)
deriving instance (Eq p, Eq e, Eq (Output e)) => Eq (Delta p e)
deriving instance (Show p, Show e, Show (Output e)) => Show (Delta p e)
instance (ToJSON p, ToJSON e, ToJSON (Output e)) => ToJSON (Delta p e)
instance (Binary p, Binary e, Binary (Output e)) => Binary (Delta p e)


{- | The class which allows for event application. -}
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

  We have a strategy for dealing with these problems: if the computation of
  an event experiences a failure on every node, then the event is pushed
  into the infimum as a failure (i.e. a no-op), but if any single node
  successfully computes the event then all other nodes will request a
  "Full Merge" from the successful node. The Full Merge will include the
  infimum __value__ computed by the successful node, which will include
  the successful application of the problematic event. The error nodes can
  thus bypass computation of the problem event altogether, and can simply
  overwrite their infimum with the infimum provided by the Full Merge.

  Doing a full merge is much more expensive than doing a simple
  'EventPack' merge, because it requires transmitting the full value of
  the 'EventFold' instead of just the outstanding operations.

  This type represents how computation of the event finished; with either a
  pure result, or some kind of system error.
-}
data EventResult e
  = SystemError (Output e)
  | Pure (Output e) (State e)


{- |
  Construct a new 'EventFold' with the given origin and initial
  participants.
-}
new :: (Default (State e), Ord p) => o -> Set p -> EventFoldF o p e f
new o participants =
  EventFold {
      psOrigin = o,
      psInfimum = Infimum {
          stateId = def,
          participants,
          stateValue = def
        },
      psEvents = mempty
    }


{- |
  Get the outstanding events that need to be propagated to a paricular
  peer.
-}
events :: (Ord p) => p -> EventFold o p e -> EventPack o p e
events peer ps =
    EventPack {
      epEvents = omitAcknowledged <$> psEvents ps,
      epOrigin = psOrigin ps,
      epInfimum = stateId (psInfimum ps)
    }
  where
    {- |
      Don't send the event data to peers which have already acknowledged
      it, saving network and cpu resources.
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
data EventPack o p e = EventPack {
     epEvents :: Map (StateId p) (Maybe (Delta p e), Set p),
     epOrigin :: o,
    epInfimum :: StateId p
  }
  deriving (Generic)
deriving instance (
    Show o, Show p, Show e, Show (Output e)
  ) =>
    Show (EventPack o p e)
instance (Show p, ToJSON o, ToJSON p, ToJSON e, ToJSON (Output e)) =>
    ToJSON (EventPack o p e)
  where
    toJSON = genericToJSON prefixedLispCase
    toEncoding = genericToEncoding prefixedLispCase
instance (
    Binary o, Binary p, Binary e, Binary (Output e)
  ) =>
    Binary (EventPack o p e)


{- |
  Monotonically merge the information in two 'EventFold's.  The resulting
  'EventFold' may have a higher infimum value, but it will never have
  a lower one. Only 'EventFold's that originated from the same 'new'
  call can be merged. If the origins are mismatched, then 'Nothing'
  is returned.
-}
mergeMaybe :: (Eq o, Event e, Ord p)
  => EventFold o p e
  -> EventPack o p e
  -> Maybe (EventFold o p e, Map (StateId p) (Output e))
mergeMaybe ps es = either (const Nothing) Just (mergeEither ps es)


{- |
  Like `mergeMaybe`, but returns a human-decipherable error message of
  exactly what went wrong.
-}
mergeEither :: (Eq o, Event e, Ord p)
  => EventFold o p e
  -> EventPack o p e
  -> Either
       (MergeError o p e)
       (EventFold o p e, Map (StateId p) (Output e))

mergeEither EventFold {psOrigin = o1} EventPack {epOrigin = o2} | o1 /= o2 =
  Left (DifferentOrigins o1 o2)

mergeEither ps pak | tooNew =
    Left (EventPackTooNew ps pak)
  where
    maxState =
      maximum
      . Set.insert (stateId . psInfimum $ ps)
      . Map.keysSet
      . psEvents
      $ ps

    tooNew :: Bool
    tooNew = maxState < epInfimum pak

mergeEither orig@(EventFold o infimum d1) ep@(EventPack d2 _ i2) =
    case
      reduce
        i2
        EventFold {
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
      Nothing -> Left (EventPackTooSparse orig ep)
      Just ps -> Right ps
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
  Like 'mergeEither', but merge a full 'EventFold' instead of just an
  event pack.
-}
fullMerge :: (Eq o, Event e, Ord p)
  => EventFold o p e
  -> EventFold o p e
  -> Either (MergeError o p e) (EventFold o p e, Map (StateId p) (Output e))
fullMerge ps (EventFold o2 i2 d2) =
  mergeEither
    ps {psInfimum = max (psInfimum ps) i2}
    EventPack {
      epOrigin = o2,
      epEvents = first (Just . runIdentity) <$> d2,
      epInfimum = stateId i2
    }


{- |
  Record the fact that the participant acknowledges the information
  contained in the 'EventFold'. The implication is that the participant
  __must__ base all future operations on the result of this function.
-}
acknowledge :: (Event e, Ord p)
  => p
  -> EventFold o p e
  -> (EventFold o p e, Map (StateId p) (Output e))
acknowledge p ps =
    {-
      First do a normal reduction, then do a special acknowledgement of the
      reduction error, if any.
    -}
    let
      (ps2, outputs) =
        runIdentity $
          reduce
            (stateId (psInfimum ps))
            ps {psEvents = fmap ackOne (psEvents ps)}
      (ps3, outputs2) = ackErr p ps2
    in
      (ps3, outputs <> outputs2)
  where
    ackOne (e, acks) = (e, Set.insert p acks)


{- | Acknowledge the reduction error, if one exists. -}
ackErr :: (Event e, Ord p)
  => p
  -> EventFold o p e
  -> (EventFold o p e, Map (StateId p) (Output e))
ackErr p ps =
  runIdentity $
    reduce
      (stateId (psInfimum ps))
      ps {
        psEvents =
          case Map.minViewWithKey (psEvents ps) of
            Just ((sid, (Identity (Error o eacks), acks)), deltas) ->
              Map.insert
                sid
                (Identity (Error o (Set.insert p eacks)), acks)
                deltas
            _ -> psEvents ps
      }


{- |
  Allow a participant to join in the distributed nature of the
  'EventFold'. Return the 'StateId' at which the participation is
  recorded, and the resulting 'EventFold'. The purpose of returning the
  state is so that it can use it to tell when the participation event
  has reached the infimum.
-}
participate :: (Ord p)
  => p
  -> p
  -> EventFold o p e
  -> (StateId p, EventFold o p e)
participate self peer ps@EventFold {psEvents} =
  let
    sid = nextId self ps
  in
    (
      sid,
      ps {
        psEvents =
          Map.insert
            sid
            (Identity (Join peer), mempty)
            psEvents
      }
    )


{- |
  Indicate that a participant is removing itself from participating in
  the distributed 'EventFold'.
-}
disassociate :: (Ord p)
  => p
  -> p
  -> EventFold o p e
  -> EventFold o p e
disassociate self peer ps@EventFold {psEvents} =
  ps {
    psEvents =
      Map.insert
        (nextId self ps)
        (Identity (UnJoin peer), mempty)
        psEvents
  }


{- |
  Introduce a change to the EventFold on behalf of the participant.
  Return the new 'EventFold' along with the projected output of the event.
-}
event :: (Ord p, Event e)
  => p
  -> e
  -> EventFold o p e
  -> (Output e, StateId p, EventFold o p e)
event p e ps@EventFold {psEvents} =
  let
    sid = nextId p ps
  in
    (
      case apply e (projectedValue ps) of
        Pure output _ -> output
        SystemError output -> output,
      sid,
      ps {
        psEvents =
          Map.insert
            sid
            (Identity (Event e), mempty)
            psEvents
      }
    )


{- | Return the current projected value of the 'EventFold'. -}
projectedValue :: (Event e) => EventFold o p e -> State e
projectedValue EventFold {psInfimum = Infimum {stateValue}, psEvents} =
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
    getDelta :: (StateId p, (Identity (Delta p e), Set p)) -> [e]
    getDelta (_, (Identity (Event e), _)) = [e]
    getDelta _ = mempty


{- | Return the current infimum value of the 'EventFold'. -}
infimumValue :: EventFoldF o p e f -> State e
infimumValue EventFold {psInfimum = Infimum {stateValue}} = stateValue


{- | Return the 'StateId' of the infimum value. -}
infimumId :: EventFoldF o p e f -> StateId p
infimumId = stateId . psInfimum


{- |
  Gets the known participants at the infimum.
-}
infimumParticipants :: EventFoldF o p e f -> Set p
infimumParticipants EventFold {psInfimum = Infimum {participants}} =
  participants


{- |
  Get all known participants. This includes participants that are
  projected for removal.
-}
allParticipants :: (Ord p) => EventFold o p e -> Set p
allParticipants EventFold {
    psInfimum = Infimum {participants},
    psEvents
  } =
    foldr updateParticipants participants (toDescList psEvents)
  where
    updateParticipants :: (Ord p)
      => (StateId p, (Identity (Delta p e), Set p))
      -> Set p
      -> Set p
    updateParticipants (_, (Identity (Join p), _)) = Set.insert p
    updateParticipants _ = id


{- |
  Get all the projected participants. This does not include participants that
  are projected for removal.
-}
projParticipants :: (Ord p) => EventFold o p e -> Set p
projParticipants EventFold {
    psInfimum = Infimum {participants},
    psEvents
  } =
    foldr updateParticipants participants (toDescList psEvents)
  where
    updateParticipants :: (Ord p)
      => (StateId p, (Identity (Delta p e), Set p))
      -> Set p
      -> Set p
    updateParticipants (_, (Identity (Join p), _)) = Set.insert p
    updateParticipants (_, (Identity (UnJoin p), _)) = Set.delete p
    updateParticipants _ = id


{- |
  Returns the participants that we think might be diverging. In this
  context, a participant is "diverging" if there is an event that
  the participant has not acknowledged but we are expecting it to
  acknowlege. Along with the participant, return the last known `StateId`
  which that participant has acknowledged.
-}
divergent :: forall o p e. (Ord p) => EventFold o p e -> Map p (StateId p)
divergent
    EventFold {
      psInfimum = Infimum {participants, stateId},
      psEvents
    }
  =
    let (byParticipant, maxSid) = sidByParticipant
    in Map.filter (< maxSid) byParticipant

  where
    sidByParticipant :: (Map p (StateId p), StateId p)
    sidByParticipant =
      foldr
        accum
        (Map.fromList [(p, stateId) | p <- Set.toList participants], stateId)
        (
          let flatten (a, (Identity b, c)) = (a, b, c)
          in (flatten <$> toAscList psEvents)
        )

    accum
      :: (StateId p, Delta p e, Set p)
      -> (Map p (StateId p), StateId p)
      -> (Map p (StateId p), StateId p)

    accum (sid, Join p, acks) (acc, maxSid) =
      (
        unionWith
          max
          (Map.insert p sid acc)
          (Map.fromList [(a, sid) | a <- Set.toList acks]),
        max maxSid sid
      )

    accum (sid, _, acks) (acc, maxSid) =
      (
        unionWith
          max
          acc
          (Map.fromList [(a, sid) | a <- Set.toList acks]),
        max maxSid sid
      )


{- | Return the origin value of the 'EventFold'. -}
origin :: EventFoldF o p e f -> o
origin = psOrigin


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
  => StateId p
     {- ^
       The infimum 'StateId' as known by some node in the cluster. "Some
       node" can be different than "this node" in the case where another
       node advanced the infimum before we did (because it knew about
       our acknowledgement, but we didn't know about its acknowledgement)
       and sent us an 'EventPack' with this value of the infimum. In this
       case, this infimum value acts as a universal acknowledgement of
       all events coming before it.
     -}
  -> EventFoldF o p e f
  -> f (EventFold o p e, Map (StateId p) (Output e))
reduce
    infState
    ps@EventFold {
      psInfimum = infimum@Infimum {participants, stateValue},
      psEvents
    }
  =
    case Map.minViewWithKey psEvents of
      Nothing ->
        pure
          (
            EventFold {
              psOrigin = psOrigin ps,
              psInfimum = psInfimum ps,
              psEvents = mempty
            },
            mempty
          )
      Just ((sid, (getUpdate, acks)), newDeltas)
        | sid <= stateId infimum -> {- The event is obsolete. Ignore it. -}
            reduce infState ps {
              psEvents = newDeltas
            }
        | isRenegade sid -> {- This is a renegade event. Ignore it. -}
            reduce infState ps {
              psEvents = newDeltas
            }
        | otherwise -> do
            implicitAcks <- unjoins sid

            update <- getUpdate
            let
              {- |
                Join events must be acknowleged by the joining peer
                before moving into the infimum.
              -}
              joining =
                case update of
                  Join p -> Set.singleton p
                  _ -> mempty
            if
                Set.null (((participants `union` joining) \\ acks) \\ implicitAcks)
                || sid <= infState
              then
                case update of
                  Join p ->
                    reduce infState ps {
                      psInfimum = infimum {
                          stateId = sid,
                          participants = Set.insert p participants
                        },
                      psEvents = newDeltas
                    }
                  UnJoin p ->
                    reduce infState ps {
                      psInfimum = infimum {
                          stateId = sid,
                          participants = Set.delete p participants
                        },
                      psEvents = newDeltas
                    }
                  Error output eacks
                    | Set.null (participants \\ eacks) -> do
                        (ps2, outputs) <-
                          reduce infState ps {
                            psInfimum = infimum {
                              stateId = sid
                            }
                          }
                        pure (ps2, Map.insert sid output outputs)
                    | otherwise -> do
                        events_ <- runEvents psEvents
                        pure
                          (
                            EventFold {
                              psOrigin = psOrigin ps,
                              psInfimum = psInfimum ps,
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
                            EventFold {
                              psOrigin = psOrigin ps,
                              psInfimum = infimum,
                              psEvents =
                                Map.insert
                                  sid
                                  (Identity (Error output mempty), acks)
                                  events_
                            },
                            mempty
                          )
                      Pure output newState -> do
                        (ps2, outputs) <-
                          reduce infState ps {
                            psInfimum = infimum {
                                stateId = sid,
                                stateValue = newState
                              },
                            psEvents = newDeltas
                          }
                        pure (ps2, Map.insert sid output outputs)
              else do
                events_ <- runEvents psEvents
                pure
                  (
                    EventFold {
                      psOrigin = psOrigin ps,
                      psInfimum = psInfimum ps,
                      psEvents = events_
                    },
                    mempty
                  )
  where
    {- | Unwrap the events from their monad. -}
    runEvents
      :: Map (StateId p) (f (Delta p e), Set p)
      -> f (Map (StateId p) (Identity (Delta p e), Set p))
    runEvents events_ =
      Map.fromList <$> sequence [
        do
          d <- fd
          pure (sid, (Identity d, acks))
        | (sid, (fd, acks)) <- Map.toList events_
      ]

    {- | Figure out which nodes have upcomming unjoins. -}
    unjoins
      :: StateId p
         {- ^
           The even under consideration, unjoins only after which we
           are interested.
         -}
      -> f (Set p)
    unjoins sid =
      Set.fromList
      . Map.elems
      . Map.filterWithKey (\k _ -> sid <= k)
      <$> unjoinMap

    {- | The static map of unjoins. -}
    unjoinMap :: f (Map (StateId p) p)
    unjoinMap =
      Map.fromList . catMaybes <$> sequence [
          update >>= \case
            UnJoin p -> pure (Just (sid, p))
            _ -> pure Nothing
          | (sid, (update, _acks)) <- Map.toList psEvents
        ]

    {- |
      Renegade events are events that originate from a non-participating
      peer.  This might happen in a network partition situation, where
      the cluster ejected a peer that later reappears on the network,
      broadcasting updates.
    -}
    isRenegade BottomSid = False
    isRenegade (Sid _ p) = not (p `member` participants)


{- |
  A utility function that constructs the next `StateId` on behalf of
  a participant.
-}
nextId :: (Ord p) => p -> EventFoldF o p e f -> StateId p
nextId p EventFold {psInfimum = Infimum {stateId}, psEvents} =
  case maximum (stateId:keys psEvents) of
    BottomSid -> Sid 0 p
    Sid ord _ -> Sid (succ ord) p


{- | Return 'True' if progress on the 'EventFold' is blocked on an error. -}
isBlockedOnError :: EventFold o p e -> Bool
isBlockedOnError ps =
  case Map.minView (psEvents ps) of
    Just ((Identity (Error _ _), _), _) -> True
    _ -> False


{- | Like 'prefixedLispCase', but leave any prefix intact. -}
lispCase :: Options
lispCase = defaultOptions {
    fieldLabelModifier = camelTo2 '-'
  }


{- | Helper for generic JSON definitions. -}
prefixedLispCase :: Options
prefixedLispCase =
    defaultOptions {
      fieldLabelModifier = camelTo2 '-' . dropPrefix,
      constructorTagModifier = camelTo2 '-' . dropPrefix
    } 
  where
    {- | Drop a standard prefix record field prefix. -}
    dropPrefix :: String -> String
    dropPrefix [] = []
    dropPrefix (x:xs)
      | isUpper x = x:xs
      | otherwise = dropPrefix xs


{- | Like 'show', but for any string-like thing. -}
showt :: (Show a, IsString b) => a -> b
showt = fromString . show


