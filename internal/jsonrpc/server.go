package jsonrpc

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
)

// maxBody bounds request bodies so a peer cannot exhaust memory.
const maxBody = 1 << 20

// Handler serves any rpc.Client (e.g. *fakenode.Node) as a JSON-RPC 2.0
// over HTTP node. Client-side errors of the wrapped rpc.Client are mapped
// back to the protocol's error codes so a remote HTTPClient observes the
// same error classes as an in-process caller.
type Handler struct {
	backend rpc.Client
}

// NewHandler wraps backend in an http.Handler speaking the chain protocol.
func NewHandler(backend rpc.Client) *Handler {
	return &Handler{backend: backend}
}

// ServeHTTP implements http.Handler. Only POST is accepted.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, 0, http.StatusMethodNotAllowed, CodeInvalidRequest, "only POST is supported")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		h.writeError(w, 0, http.StatusBadRequest, CodeParseError, "cannot read request body")
		return
	}
	var req Request
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, 0, http.StatusOK, CodeParseError, "malformed JSON: "+err.Error())
		return
	}
	if req.JSONRPC != "2.0" {
		h.writeError(w, req.ID, http.StatusOK, CodeInvalidRequest, `jsonrpc must be "2.0"`)
		return
	}

	switch req.Method {
	case MethodChainTip:
		h.serveTip(w, r, req)
	case MethodGetBlockByNum:
		h.serveBlock(w, r, req)
	default:
		h.writeError(w, req.ID, http.StatusOK, CodeMethodNotFound, "unknown method "+req.Method)
	}
}

func (h *Handler) serveTip(w http.ResponseWriter, r *http.Request, req Request) {
	tip, err := h.backend.ChainTip(r.Context())
	if err != nil {
		h.writeBackendError(w, req.ID, err)
		return
	}
	h.writeResult(w, req.ID, mustJSON(encodeUint(tip)))
}

func (h *Handler) serveBlock(w http.ResponseWriter, r *http.Request, req Request) {
	var params []string
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) != 1 {
		h.writeError(w, req.ID, http.StatusOK, CodeInvalidParams, `params must be ["0x<height>"]`)
		return
	}
	height, err := decodeUint(params[0])
	if err != nil {
		h.writeError(w, req.ID, http.StatusOK, CodeInvalidParams, "bad height "+params[0])
		return
	}
	b, err := h.backend.BlockByNumber(r.Context(), height)
	if err != nil {
		h.writeBackendError(w, req.ID, err)
		return
	}
	h.writeResult(w, req.ID, mustJSON(blockToDTO(b)))
}

// writeBackendError maps a classified rpc error to the protocol's codes.
func (h *Handler) writeBackendError(w http.ResponseWriter, id int64, err error) {
	code := CodeInternal
	switch {
	case errors.Is(err, rpc.ErrBlockNotFound):
		code = CodeBlockNotFound
	default:
		switch rpc.ClassOf(err) {
		case rpc.ClassTimeout:
			code = CodeTimeout
		case rpc.ClassRateLimited:
			code = CodeRateLimited
		case rpc.ClassTemporary:
			code = CodeServerError
		case rpc.ClassPermanent:
			code = CodeInvalidRequest
		}
	}
	h.writeError(w, id, http.StatusOK, code, err.Error())
}

func (h *Handler) writeResult(w http.ResponseWriter, id int64, result json.RawMessage) {
	h.write(w, http.StatusOK, Response{JSONRPC: "2.0", ID: id, Result: result})
}

func (h *Handler) writeError(w http.ResponseWriter, id int64, status, code int, msg string) {
	h.write(w, status, Response{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &ErrorObject{Code: code, Message: msg},
	})
}

func (h *Handler) write(w http.ResponseWriter, status int, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}
