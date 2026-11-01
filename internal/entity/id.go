package entity

import "encoding/hex"

const IDBytes = 16

func ValidID(id string) bool {
	if len(id) != IDBytes*2 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}
