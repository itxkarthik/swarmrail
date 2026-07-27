package peer

import (
	"errors"
	"io"
	"net"
	"time"
)

var ErrInvalidHandshake = errors.New("invalid handshake")

const handshakeLength = 68

func buildHandshake(infoHash [20]byte, peerID [20]byte) []byte {
	buf := make([]byte, handshakeLength)
	buf[0] = 19
	copy(buf[1:20], []byte("BitTorrent protocol"))
	copy(buf[28:48], infoHash[:])
	copy(buf[48:68], peerID[:])
	return buf
}

func readHandshake(conn net.Conn, expectedInfoHash [20]byte) (remotePeerID [20]byte, err error) {
	resp := make([]byte, handshakeLength)
	if _, err = io.ReadFull(conn, resp); err != nil {
		return remotePeerID, err
	}
	const protocol = "BitTorrent protocol"
	if resp[0] != byte(len(protocol)) || string(resp[1:20]) != protocol {
		return remotePeerID, ErrInvalidHandshake
	}
	var receivedInfoHash [20]byte
	copy(receivedInfoHash[:], resp[28:48])
	if receivedInfoHash != expectedInfoHash {
		return remotePeerID, ErrInvalidHandshake
	}
	copy(remotePeerID[:], resp[48:68])
	return remotePeerID, nil
}

// Handshake sends a handshake message to an outbound peer and validates its response.
func Handshake(conn net.Conn, infoHash [20]byte, peerID [20]byte) (remotePeerID [20]byte, err error) {
	if err = conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return remotePeerID, err
	}
	defer conn.SetDeadline(time.Time{})
	if _, err = conn.Write(buildHandshake(infoHash, peerID)); err != nil {
		return remotePeerID, err
	}
	return readHandshake(conn, infoHash)
}

// AcceptHandshake validates an inbound peer before returning this client's handshake.
func AcceptHandshake(conn net.Conn, infoHash [20]byte, peerID [20]byte) (remotePeerID [20]byte, err error) {
	if err = conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return remotePeerID, err
	}
	defer conn.SetDeadline(time.Time{})
	remotePeerID, err = readHandshake(conn, infoHash)
	if err != nil {
		return remotePeerID, err
	}
	_, err = conn.Write(buildHandshake(infoHash, peerID))
	return remotePeerID, err
}
