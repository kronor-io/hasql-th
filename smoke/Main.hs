{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- |
-- A module that exists to be compiled. Every binding below is a quasiquote
-- using one of the extensions, given an explicit type signature that the
-- generated code has to agree with, so that a wrong mapping lambda is a
-- compile error rather than something to eyeball.
--
-- Built with @-ddump-splices@; the dump is checked in as @smoke/splices.txt@.
module Main (main) where

import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Vector (Vector)
import Hasql.Statement (Statement)
import Hasql.TH

-- * The Haskell side

data User = User
  { userId :: Int64,
    userName :: Text
  }
  deriving (Show)

defaultUser :: User
defaultUser = User 0 "anonymous"

mkUser :: Int64 -> Text -> User
mkUser = User

widen :: Int32 -> Int64
widen = fromIntegral

data Flag = Flag
  deriving (Show)

data Note = Note Int64 (Maybe Text)
  deriving (Show)

mkNote :: Int64 -> Maybe Text -> Note
mkNote = Note

data Merchant = Merchant
  { merchantId :: Int32,
    signatureKey :: Text
  }

-- * The statements

-- | Function target with arguments. One target over two columns, so the
-- mapping lambda must be @\\(fn1, fn2) -> mkUser fn1 fn2@ and not wrap the
-- result in a one-element tuple.
functionTarget :: Statement () User
functionTarget =
  [singletonStatement|
    select $mkUser (id :: int8, name :: text) from "user"
    |]

-- | Nullary target: a bare value, consuming no columns. Upper case, so it
-- becomes a @ConE@ and the @all VarE@ guard does not swallow the mapping.
--
-- Two sibling columns rather than one, because @mkMapping@ takes the
-- single-column branch whenever the statement returns one column and that
-- branch keeps only the first target. A nullary target next to exactly one
-- SQL column therefore drops the column; that is the 0.4 fork's behaviour,
-- preserved here deliberately.
nullaryTarget :: Statement () (Flag, Int64, Text)
nullaryTarget =
  [singletonStatement|
    select $Flag (), id :: int8, name :: text from "user"
    |]

-- | Upper-case name is a data constructor.
constructorTarget :: Statement () User
constructorTarget =
  [singletonStatement|
    select $User (id :: int8, name :: text) from "user"
    |]

-- | Record construction.
recordConstruction :: Statement () User
recordConstruction =
  [singletonStatement|
    select $User {userId = id :: int8, userName = name :: text} from "user"
    |]

-- | Record update: lower-case name, so @RecUpdE@ over the named value.
recordUpdate :: Statement () User
recordUpdate =
  [singletonStatement|
    select $defaultUser {userName = name :: text} from "user"
    |]

-- | A function target nested inside a record field.
nestedTarget :: Statement () User
nestedTarget =
  [singletonStatement|
    select $User {userId = $widen (id :: int4), userName = name :: text} from "user"
    |]

-- | Haskell and plain SQL targets in one select list.
mixedTargets :: Statement () (User, Text)
mixedTargets =
  [singletonStatement|
    select $mkUser (id :: int8, name :: text), created :: text from "user"
    |]

-- | Two fields off one parameter plus a bare second parameter. The statement
-- takes @(Merchant, Text)@, not three separate parameters.
paramFields :: Statement (Merchant, Text) Int64
paramFields =
  [singletonStatement|
    select id :: int8 from "user"
    where merchant = $1.$merchantId :: int4
      and key = $1.$signatureKey :: text
      and status = $2 :: text
    |]

-- | A single parameter with a field is a bare lambda pattern, not a
-- one-element tuple pattern.
singleParamField :: Statement Merchant Int64
singleParamField =
  [singletonStatement|
    select id :: int8 from "user" where merchant = $1.$merchantId :: int4
    |]

-- | Nullability marker inside a Haskell target: @text?@ decodes to
-- @Maybe Text@, so @mkNote@ is applied at that type.
nullableInTarget :: Statement () Note
nullableInTarget =
  [singletonStatement|
    select $mkNote (id :: int8, note :: text?) from "note"
    |]

-- | No extension at all. The splice for this one must be exactly what
-- upstream emits: no @fmap@, no @contramap@.
plainStatement :: Statement (Int32, Text) (Vector (Int64, Text))
plainStatement =
  [vectorStatement|
    select id :: int8, name :: text from "user" where merchant = $1 :: int4 and status = $2 :: text
    |]

main :: IO ()
main =
  mapM_
    putStrLn
    [ "functionTarget, nullaryTarget, constructorTarget, recordConstruction,",
      "recordUpdate, nestedTarget, mixedTargets, paramFields, singleParamField,",
      "nullableInTarget and plainStatement all compiled."
    ]
