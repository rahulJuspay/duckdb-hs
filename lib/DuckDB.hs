{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module DuckDB 
where

import Foreign
    ( Word8,
      Ptr,
      Storable(poke),
      plusPtr,
      newForeignPtr,
      finalizerFree,
      malloc,
      finalizeForeignPtr,
      touchForeignPtr,
      withForeignPtr,
      FinalizerPtr,
      ForeignPtr )
import Foreign.C.Types ( CSize, CInt )
import Foreign.ForeignPtr.Unsafe(unsafeForeignPtrToPtr)
import Foreign.Marshal.Alloc
import Foreign.Ptr
import Foreign.C.String
import Foreign.Storable
import Data.ByteString.Internal as BS
    ( mallocByteString, ByteString(PS) )
import DuckDB.FFI
import qualified Data.ByteString as B
import Control.Exception
import Conduit
import Data.Aeson
import Data.Aeson.Lens
import qualified Data.Aeson.KeyMap as DAKM
import Data.Traversable
import Control.Lens
import qualified Data.Text as DT
import qualified Data.Aeson.Key as Key
import Data.Int
import Data.Word
-- import Control.Lens.Combinators ( makeLenses, at, lmap, review)
-- import Control.Lens.Operators ( (<&>), (^.), (&), (.~), (?~))
import Control.Monad

-- import Data.HashMap.Strict 
{-# LANGUAGE OverloadedStrings #-}
data DuckDbCon = DuckDbCon {
  connection :: Ptr DuckDBConnection
  , database :: Ptr DuckDBDatabase
  , config :: Maybe (Ptr DuckDBConfig)
}

data DuckDBType
    = Invalid
    | Boolean
    | TinyInt
    | SmallInt
    | DuckInteger
    | BigInt
    | UTinyInt
    | USmallInt
    | UInteger
    | UBigInt
    | Float
    | DuckDouble
    | Timestamp
    | Date
    | Time
    | Interval
    | HugeInt
    | Varchar
    | Blob
    | Decimal
    | TimestampS
    | TimestampMS
    | TimestampNS
    | Enum
    | List
    | Struct
    | Map
    | UUID
    | Union
    | Bit
    | TimeTZ
    | TimestampTZ
    | UHugeInt
    | Array
    | Any
    | VarInt
    | SQLNull
    deriving (Show, Eq, Enum)

duckdbOpen :: Maybe String -> IO (Ptr DuckDBDatabase)
duckdbOpen mPath = do
  outDatabase <- malloc
  cPath <- maybe (pure nullPtr) newCString mPath
  result <- c_duckdb_open cPath outDatabase
  if result == 0
    then pure outDatabase
    else error "Failed to open DuckDB database."

duckdbOpenExt :: Maybe String -> DuckDBConfig -> IO (Ptr DuckDBDatabase)
duckdbOpenExt mPath config = do
  outDatabase <- malloc
  cPath <- maybe (pure nullPtr) newCString mPath
  result <- c_duckdb_open_ext cPath outDatabase config nullPtr
  if result == 0
    then pure outDatabase
    else error "Failed to open DuckDB database."

getConfigFromHM :: (Traversable t) => t (String, String) -> IO (Ptr DuckDBConfig)
getConfigFromHM items = do
  config <- duckdbCreateConfig
  configPtr <- peek config
  traverse (\(key,value) -> duckdbSetConfig configPtr key value) items
  pure config

duckdbConnect :: Ptr LDuckDBDatabase -> IO (Ptr DuckDBConnection)
duckdbConnect dataBase = do
  outConnection <- malloc
  result <- c_duckdb_connect dataBase outConnection
  if result == 0
    then pure outConnection
    else error "Failed to Connect to DuckDB database."

duckdbOpenAndConnect :: (Traversable t) => Maybe String -> Maybe (t (String, String)) -> IO DuckDbCon
duckdbOpenAndConnect mPath mConfigItems = do
  (ptr, config) <- case mConfigItems of
          Just items -> do 
            config <-  getConfigFromHM items
            configPtr <- peek config
            dbptr <- duckdbOpenExt mPath configPtr
            pure (dbptr, Just config)
          Nothing -> do
            dbptr <- duckdbOpen mPath
            pure (dbptr, Nothing)
  db <- peek ptr
  con <-  duckdbConnect db
  pure $ DuckDbCon con ptr config

duckdbQuery :: DuckDbCon -> String -> IO (Ptr DuckDBResult)
duckdbQuery DuckDbCon{connection} query = do
  alloca $ \resPtr -> do 
    cquery <- newCString query
    con <- peek connection
    result <- c_duckdb_query con cquery resPtr
    if result == 0
      then pure resPtr
      else do
        errorString <- peekCString $ c_duckdb_result_error resPtr
        error errorString

getMappedValues col idxRow columnsData types = do
  case (types !! col) of
    Boolean -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Bool) (idxRow)
    TinyInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Int8) (idxRow)
    SmallInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Int16) (idxRow)
    DuckInteger -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Int32) idxRow
    BigInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Int64) (idxRow)
    UTinyInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Word8) (idxRow)
    USmallInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Word16) (idxRow)
    UInteger -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Word32) (idxRow)
    UBigInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Word64) (idxRow)
    Float -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Float) (idxRow)
    DuckDouble -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Double) (idxRow)
    Varchar -> toJSON <$> peekCString (c_duckdb_cstring_from_struct_string (fst (columnsData !! col)) (toEnum idxRow))
    Timestamp -> pure $ toJSON $ fromEnum (c_duckdb_timestamp_from_struct (fst (columnsData !! col)) (toEnum idxRow))
    _ -> pure $ toJSON Null

getRowData :: Ptr LDuckDBDataChunk -> [DuckDBType] -> Int -> [String] -> ConduitT () Object IO ()
getRowData chunk types numCols cNames = do
  let numRows = fromEnum $ c_duckdb_data_chunk_get_size chunk
  columnsData <- mapM (\idxCol -> do
    let 
      vector = c_duckdb_data_chunk_get_vector chunk (toEnum idxCol)
      colData = c_duckdb_vector_get_data vector
      validatyCol = c_duckdb_vector_get_validity vector
    pure (colData , validatyCol)
     ) [0..(numCols-1)]
  forM_ [0..(numRows-1)] (\idxRow -> do
    let 
      obj = mempty :: Object
    finalObj <- foldM (\o col -> do
      val <- if (fromEnum (c_duckdb_validity_row_is_valid (snd (columnsData !! col)) (toEnum idxRow)) == 1 ) 
              then liftIO $ Just <$> getMappedValues col idxRow columnsData types
              else pure Nothing
      pure $ (o 
                & at (Key.fromText $ DT.pack (cNames !! col)) ?~ (toJSON val))
       ) obj [0..(numCols-1)]
    yield finalObj
    )

makeResultConduit :: Ptr DuckDBResult -> ConduitT () Object IO ()
makeResultConduit resultPtr = do
  liftIO $ print "in res sonduit"
  let 
    numCols = fromEnum $ c_duckdb_column_count resultPtr
    types = map (\idx -> toEnum (fromEnum $ c_duckdb_column_type resultPtr (toEnum idx)) :: DuckDBType) [0..(numCols-1)]
    loopFetch cNames= do
      chunkPtr <- liftIO $ c_duckdb_stream_fetch_chunk_ptr resultPtr
      if chunkPtr == nullPtr 
        then pure ()
        else do
          getRowData chunkPtr types numCols cNames
          loopFetch cNames
  liftIO $ print types
  colNames <- liftIO $ mapM (\idx -> peekCString $ c_duckdb_column_name resultPtr (toEnum idx)) [0..(numCols-1)]
  loopFetch colNames
  

duckdbQueryConduitRes :: DuckDbCon -> String -> ConduitT () Object IO ()
duckdbQueryConduitRes DuckDbCon{connection} query = do
  resPtr <- liftIO $ malloc
  psPtr <- liftIO $ malloc
  cquery <- liftIO $ newCString query
  con <- liftIO $ peek connection
  liftIO $ c_duckdb_prepare con cquery psPtr
  ps <- liftIO $ peek psPtr
  result <- liftIO $ 
              c_duckdb_execute_prepared_streaming ps resPtr
  liftIO $ print result
  if result == 0
    then makeResultConduit resPtr
    else do
        liftIO $ print "in error"
        liftIO $ print (c_duckdb_result_error resPtr)
        -- errorString <- liftIO $ peekCString $ c_duckdb_result_error resPtr
        -- error errorString

duckdbRowCount :: Ptr DuckDBResult -> Int
duckdbRowCount resultPtr = fromEnum $ c_duckdb_row_count resultPtr

duckdbDisconnectAndClose :: DuckDbCon ->  IO ()
duckdbDisconnectAndClose DuckDbCon{connection, database, config} = do
  c_duckdb_disconnect connection
  c_duckdb_close database
  maybe (pure ()) duckdbDistroyConfig config


duckdbCreateConfig :: IO (Ptr DuckDBConfig)
duckdbCreateConfig = do
  configPtr <- malloc
  result <- c_duckdb_create_config configPtr
  if result == 0
    then pure configPtr
    else error "Failed to create config."

duckdbSetConfig :: DuckDBConfig -> String -> String -> IO ()
duckdbSetConfig configPtr a b =  do
    key <- newCString a
    value <- newCString b
    result <- c_duckdb_set_config configPtr key value 
    if result == 0
      then pure ()
      else error "Failed to set config."

duckdbDistroyConfig :: Ptr DuckDBConfig -> IO ()
duckdbDistroyConfig configPtr = c_duckdb_destroy_config configPtr
