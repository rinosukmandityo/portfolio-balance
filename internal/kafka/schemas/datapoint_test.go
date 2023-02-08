package schemas

import (
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func TestDatapoint(t *testing.T) {
	codec, err := goavro.NewCodec(DataPointSchema)
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(1 * time.Second)
	period := now.Add(-time.Minute)

	// generate avro msg
	var bin []byte
	bin, err = codec.BinaryFromNative(nil, map[string]interface{}{
		"user_id":  "USER001",
		"currency": "PHP",
		"period":   period,
		"amount":   "1234.56789",
		"sent_at":  now,
	})
	require.NoError(t, err)

	var native interface{}
	native, _, err = codec.NativeFromBinary(bin)
	require.NoError(t, err)

	decoded, ok := native.(map[string]interface{})
	require.True(t, ok)

	parsed, err := DecodeDataPoint(decoded)
	require.NoError(t, err)

	require.Equal(t, "USER001", parsed.UserID)
	require.Equal(t, "PHP", parsed.Currency)
	require.Equal(t, period.String(), parsed.Timestamp.String())
	require.Equal(t, "1234.56789", parsed.Amount.String())
	require.Equal(t, now.String(), parsed.SentAt.String())
}
