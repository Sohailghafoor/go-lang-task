package kvstore

import (
	"fmt"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// Define the module name
const ModuleName = "kvstore"

// Define the key-value pair data structure
type KeyValuePair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Define the module message types and their corresponding handlers
type MsgCreateKeyValuePair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewMsgCreateKeyValuePair(key, value string) MsgCreateKeyValuePair {
	return MsgCreateKeyValuePair{
		Key:   key,
		Value: value,
	}
}

func (msg MsgCreateKeyValuePair) Route() string { return ModuleName }

func (msg MsgCreateKeyValuePair) Type() string { return "create_key_value_pair" }

func (msg MsgCreateKeyValuePair) ValidateBasic() error {
	if len(msg.Key) == 0 || len(msg.Value) == 0 {
		return fmt.Errorf("key and value cannot be empty")
	}
	return nil
}

func (msg MsgCreateKeyValuePair) GetSignBytes() []byte {
	return sdk.MustSortJSON(ModuleCdc.MustMarshalJSON(&msg))
}

func (msg MsgCreateKeyValuePair) GetSigners() []sdk.AccAddress {
	return []sdk.AccAddress{sdk.AccAddress(msg.From)}
}

func handleMsgCreateKeyValuePair(ctx sdk.Context, k Keeper, msg MsgCreateKeyValuePair) (*sdk.Result, error) {
	if err := k.CreateKeyValuePair(ctx, msg.Key, msg.Value); err != nil {
		return nil, err
	}
	return &sdk.Result{Events: ctx.EventManager().Events()}, nil
}

// Define the module query types and their corresponding handlers
type QueryGetKeyValuePair struct {
	Key string `json:"key"`
}

func NewQueryGetKeyValuePair(key string) QueryGetKeyValuePair {
	return QueryGetKeyValuePair{
		Key: key,
	}
}

func (query QueryGetKeyValuePair) Route() string { return ModuleName }

func (query QueryGetKeyValuePair) Type() string { return "get_key_value_pair" }

func (query QueryGetKeyValuePair) ValidateBasic() error {
	if len(query.Key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	return nil
}

func handleQueryGetKeyValuePair(ctx sdk.Context, k Keeper, query QueryGetKeyValuePair) ([]byte, error) {
	value, err := k.GetKeyValuePair(ctx, query.Key)
	if err != nil {
		return nil, err
	}
	return ModuleCdc.MustMarshalJSON(value), nil
}

// Define the module keeper to manage the state of the module
type Keeper struct {
	keyValueStoreKey sdk.StoreKey
	cdc              *codec.Codec
}

func NewKeeper(keyValueStoreKey sdk.StoreKey, cdc *codec.Codec) Keeper {
	return Keeper{
		keyValueStoreKey: keyValueStoreKey,
		cdc:              cdc,
	}
}

func (k Keeper) KeyValueExists(ctx sdk.Context, key string) bool {
	store := ctx.KVStore(k.keyValueStoreKey)
	return store.Has([]byte(key))
}

func (k Keeper) GetKeyValuePair(ctx sdk.Context, key string) (*KeyValuePair, error) {
	store := ctx.KVStore(k.keyValueStoreKey)
	if !k.KeyValueExists(ctx, key) {
		return nil, fmt.Errorf("key-value pair not found")
	}
	value := store.Get([]byte(key))
	var keyValuePair KeyValuePair
	k.cdc.MustUnmarshalBinaryBare(value, &keyValuePair)
	return &keyValuePair, nil
}

func (k Keeper) SetKeyValuePair(ctx sdk.Context, keyValuePair KeyValuePair) {
	store := ctx.KVStore(k.keyValueStoreKey)
	store.Set([]byte(keyValuePair.Key), k.cdc.MustMarshalBinaryBare(&keyValuePair))
}

// Register the module and its types with the SDK
func RegisterCodec(cdc *codec.Codec) {
	cdc.RegisterConcrete(MsgCreateKeyValuePair{}, "kvstore/CreateKeyValuePair", nil)
	cdc.RegisterConcrete(QueryGetKeyValuePair{}, "kvstore/GetKeyValuePair", nil)
	cdc.RegisterConcrete(KeyValuePair{}, "kvstore/KeyValuePair", nil)
}

func NewQuerier(k Keeper) sdk.Querier {
	return func(ctx sdk.Context, path []string, req abci.RequestQuery) ([]byte, error) {
		switch path[0] {
		case "get":
			return queryGetKeyValuePair(ctx, path[1:], req, k)
		default:
			return nil, sdkerrors.Wrap(sdkerrors.ErrUnknownRequest, "unknown kvstore query endpoint")
		}
	}
}

func queryGetKeyValuePair(ctx sdk.Context, path []string, req abci.RequestQuery, k Keeper) ([]byte, error) {
	key := path[0]
	query := NewQueryGetKeyValuePair(key)
	bz, err := k.cdc.MarshalJSON(query)
	if err != nil {
		return nil, err
	}
	res, err := abciQueryWithData(ctx, "custom/kvstore/get", bz)
	if err != nil {
		return nil, err
	}
	if res.Code != abci.CodeTypeOK {
		return nil, sdkerrors.Wrap(sdkerrors.ErrUnknownRequest, res.Log)
	}
	var value KeyValuePair
	if err := k.cdc.UnmarshalBinaryBare(res.Value, &value); err != nil {
		return nil, err
	}
	return k.cdc.MustMarshalJSON(value), nil
}

func abciQueryWithData(ctx sdk.Context, path string, data []byte) (abci.ResponseQuery, error) {
	req := abci.RequestQuery{
		Path: path,
		Data: data,
	}
	return ctx.Query(req)
}
