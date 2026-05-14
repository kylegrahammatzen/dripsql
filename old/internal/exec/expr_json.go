package exec

import (
	"encoding/json"
	"fmt"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
)

func evalProjectJSONPath(left any, op v3sql.BoundOp, right any) (any, bool, error) {
	leftText, ok := left.(string)
	if !ok {
		return nil, false, nil
	}
	var node any
	if err := json.Unmarshal([]byte(leftText), &node); err != nil {
		return nil, false, nil
	}
	child, ok := jsonPathLookup(node, right)
	if !ok {
		return nil, false, nil
	}
	if op == v3sql.BoundOpJSONGetText {
		return jsonAsText(child)
	}
	encoded, err := json.Marshal(child)
	if err != nil {
		return nil, false, fmt.Errorf("project JSON encode: %w", err)
	}
	return string(encoded), true, nil
}

func jsonPathLookup(node any, key any) (any, bool) {
	switch keyValue := key.(type) {
	case string:
		obj, ok := node.(map[string]any)
		if !ok {
			return nil, false
		}
		child, ok := obj[keyValue]
		return child, ok
	default:
		idx, ok := projectIntValue(key)
		if !ok {
			return nil, false
		}
		arr, ok := node.([]any)
		if !ok {
			return nil, false
		}
		if idx < 0 {
			idx += int64(len(arr))
		}
		if idx < 0 || idx >= int64(len(arr)) {
			return nil, false
		}
		return arr[idx], true
	}
}

func jsonAsText(node any) (any, bool, error) {
	switch value := node.(type) {
	case nil:
		return nil, false, nil
	case string:
		return value, true, nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, false, fmt.Errorf("project JSON encode: %w", err)
		}
		return string(encoded), true, nil
	}
}
