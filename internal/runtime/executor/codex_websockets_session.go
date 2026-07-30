package executor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type codexWebsocketSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*codexWebsocketSession
}

var globalCodexWebsocketSessionStore = &codexWebsocketSessionStore{
	sessions: make(map[string]*codexWebsocketSession),
}

type websocketConnectionCloser struct {
	conn *websocket.Conn
	once sync.Once
	err  error
}

func newWebsocketConnectionCloser(conn *websocket.Conn) *websocketConnectionCloser {
	if conn == nil {
		return nil
	}
	return &websocketConnectionCloser{conn: conn}
}

func (c *websocketConnectionCloser) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.once.Do(func() {
		c.err = c.conn.Close()
	})
	return c.err
}

type codexWebsocketSession struct {
	sessionID string

	reqMu sync.Mutex

	connMu          sync.Mutex
	conn            *websocket.Conn
	connCloser      *websocketConnectionCloser
	wsURL           string
	authID          string
	lifecycleBindMu sync.Mutex
	lifecycle       cliproxyexecutor.ExecutionLifecycle
	lifecycleModel  string

	writeMu sync.Mutex

	activeMu     sync.Mutex
	activeConn   *websocket.Conn
	activeCh     chan codexWebsocketRead
	activeDone   <-chan struct{}
	activeCancel context.CancelFunc
	activeTurn   *codexWebsocketTurnStats
	turnSequence uint64

	readerConn *websocket.Conn

	upstreamDisconnectOnce    sync.Once
	upstreamDisconnectCh      chan error
	upstreamDisconnectErrMu   sync.RWMutex
	upstreamDisconnectErrConn *websocket.Conn
	upstreamDisconnectErr     error
}

type codexWebsocketRead struct {
	conn    *websocket.Conn
	msgType int
	payload []byte
	err     error
}

type codexWebsocketReadStats struct {
	startedAt   time.Time
	lastFrameAt time.Time
	lastEvent   string
	frameCount  uint64
	byteCount   uint64
}

const codexWebsocketActivityLogInterval = 10 * time.Second

type codexWebsocketTurnStats struct {
	mu sync.Mutex

	turnID               uint64
	startedAt            time.Time
	lastFrameAt          time.Time
	lastActivityLoggedAt time.Time
	lastEvent            string
	frameCount           uint64
	byteCount            uint64
}

func newCodexWebsocketTurnStats(turnID uint64, startedAt time.Time) *codexWebsocketTurnStats {
	return &codexWebsocketTurnStats{turnID: turnID, startedAt: startedAt}
}

func (s *codexWebsocketTurnStats) observeTextFrame(now time.Time, payload []byte) (log.Fields, bool, bool) {
	if s == nil {
		return nil, false, false
	}
	eventType := safeCodexWebsocketEventType(gjson.GetBytes(payload, "type").String())
	s.mu.Lock()
	previousGap := time.Duration(0)
	if !s.lastFrameAt.IsZero() {
		previousGap = now.Sub(s.lastFrameAt)
	}
	firstFrame := s.frameCount == 0
	eventChanged := firstFrame || eventType != s.lastEvent
	s.frameCount++
	s.byteCount += uint64(len(payload))
	s.lastFrameAt = now
	s.lastEvent = eventType
	logActivity := !eventChanged && (s.lastActivityLoggedAt.IsZero() ||
		now.Sub(s.lastActivityLoggedAt) >= codexWebsocketActivityLogInterval)
	if eventChanged || logActivity {
		s.lastActivityLoggedAt = now
	}
	fields := s.fieldsLocked(now)
	fields["event"] = eventType
	fields["sequence"] = gjson.GetBytes(payload, "sequence_number").Int()
	fields["frame_bytes"] = len(payload)
	fields["turn_previous_gap"] = previousGap
	fields["first_frame"] = firstFrame
	addCodexWebsocketItemMetadata(fields, payload)
	s.mu.Unlock()
	return fields, eventChanged, logActivity
}

func (s *codexWebsocketTurnStats) fields(now time.Time) log.Fields {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	fields := s.fieldsLocked(now)
	s.mu.Unlock()
	return fields
}

func (s *codexWebsocketTurnStats) fieldsLocked(now time.Time) log.Fields {
	fields := log.Fields{
		"turn":                s.turnID,
		"turn_started_at":     s.startedAt.Format(time.RFC3339Nano),
		"turn_elapsed":        now.Sub(s.startedAt),
		"turn_last_event":     "<none>",
		"turn_last_frame_ago": time.Duration(0),
		"turn_frame_count":    s.frameCount,
		"turn_byte_count":     s.byteCount,
		"observed_at":         now.Format(time.RFC3339Nano),
	}
	if s.lastEvent != "" {
		fields["turn_last_event"] = s.lastEvent
	}
	if !s.lastFrameAt.IsZero() {
		fields["turn_last_frame_ago"] = now.Sub(s.lastFrameAt)
	}
	return fields
}

func (s *codexWebsocketReadStats) observeTextFrame(now time.Time, payload []byte) (log.Fields, bool) {
	if s == nil {
		return nil, false
	}
	if s.startedAt.IsZero() {
		s.startedAt = now
	}
	eventType := safeCodexWebsocketEventType(gjson.GetBytes(payload, "type").String())
	previousGap := time.Duration(0)
	if !s.lastFrameAt.IsZero() {
		previousGap = now.Sub(s.lastFrameAt)
	}
	s.frameCount++
	s.byteCount += uint64(len(payload))
	s.lastFrameAt = now
	changed := eventType != s.lastEvent
	s.lastEvent = eventType
	fields := log.Fields{
		"event":              eventType,
		"sequence":           gjson.GetBytes(payload, "sequence_number").Int(),
		"frame_bytes":        len(payload),
		"previous_gap":       previousGap,
		"frame_count":        s.frameCount,
		"cumulative_size":    s.byteCount,
		"observed_at":        now.Format(time.RFC3339Nano),
		"connection_elapsed": now.Sub(s.startedAt),
	}
	addCodexWebsocketItemMetadata(fields, payload)
	return fields, changed
}

func safeCodexWebsocketEventType(eventType string) string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return "<unknown>"
	}
	if len(eventType) > 128 {
		return "<invalid>"
	}
	for _, r := range eventType {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == '-' {
			continue
		}
		return "<invalid>"
	}
	return eventType
}

func addCodexWebsocketItemMetadata(fields log.Fields, payload []byte) {
	if fields == nil {
		return
	}
	if outputIndex := gjson.GetBytes(payload, "output_index"); outputIndex.Exists() {
		fields["output_index"] = outputIndex.Int()
	}
	if itemType := gjson.GetBytes(payload, "item.type"); itemType.Exists() {
		fields["item_type"] = safeCodexWebsocketEventType(itemType.String())
	}
}

func (s *codexWebsocketReadStats) readStopFields(now time.Time, active bool, err error) log.Fields {
	fields := log.Fields{
		"active_response":    active,
		"last_event":         "<none>",
		"last_frame_ago":     time.Duration(0),
		"frame_count":        uint64(0),
		"byte_count":         uint64(0),
		"error_kind":         codexWebsocketReadErrorKind(err),
		"observed_at":        now.Format(time.RFC3339Nano),
		"connection_elapsed": time.Duration(0),
	}
	if s != nil {
		if s.lastEvent != "" {
			fields["last_event"] = s.lastEvent
		}
		if !s.lastFrameAt.IsZero() {
			fields["last_frame_ago"] = now.Sub(s.lastFrameAt)
		}
		fields["frame_count"] = s.frameCount
		fields["byte_count"] = s.byteCount
		if !s.startedAt.IsZero() {
			fields["connection_elapsed"] = now.Sub(s.startedAt)
		}
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		fields["close_code"] = closeErr.Code
	}
	return fields
}

func codexWebsocketReadErrorKind(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline"
	}
	if errors.Is(err, net.ErrClosed) {
		return "local_close"
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		if closeErr.Code == websocket.CloseAbnormalClosure {
			return "abnormal_close"
		}
		return "websocket_close"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "read_error"
}

func (s *codexWebsocketSession) setActive(conn *websocket.Conn, ch chan codexWebsocketRead) {
	s.setActiveWithCodexTurn(conn, ch, nil)
}

func (s *codexWebsocketSession) setActiveWithCodexTurn(
	conn *websocket.Conn,
	ch chan codexWebsocketRead,
	turn *codexWebsocketTurnStats,
) {
	if s == nil {
		return
	}
	s.activeMu.Lock()
	if s.activeCancel != nil {
		s.activeCancel()
		s.activeCancel = nil
		s.activeDone = nil
	}
	s.activeConn = conn
	s.activeCh = ch
	s.activeTurn = turn
	if conn != nil && ch != nil {
		activeCtx, activeCancel := context.WithCancel(context.Background())
		s.activeDone = activeCtx.Done()
		s.activeCancel = activeCancel
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) activate(conn *websocket.Conn) chan codexWebsocketRead {
	if s == nil || conn == nil {
		return nil
	}
	ch := make(chan codexWebsocketRead, 4096)
	s.setActive(conn, ch)
	return ch
}

func (s *codexWebsocketSession) activateCodexTurn(conn *websocket.Conn) chan codexWebsocketRead {
	if s == nil || conn == nil {
		return nil
	}
	now := time.Now()
	ch := make(chan codexWebsocketRead, 4096)
	s.activeMu.Lock()
	s.turnSequence++
	turn := newCodexWebsocketTurnStats(s.turnSequence, now)
	s.activeMu.Unlock()
	s.setActiveWithCodexTurn(conn, ch, turn)
	fields := turn.fields(now)
	fields["session"] = s.sessionID
	log.WithFields(fields).Info("codex websocket upstream turn activated")
	return ch
}

func (s *codexWebsocketSession) logCodexTurnSent(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	_, _, turn := s.activeCodexForConn(conn)
	if turn == nil {
		return
	}
	fields := turn.fields(time.Now())
	fields["session"] = s.sessionID
	log.WithFields(fields).Info("codex websocket upstream turn sent")
}

func (s *codexWebsocketSession) activeForConn(conn *websocket.Conn) (chan codexWebsocketRead, <-chan struct{}) {
	ch, done, _ := s.activeCodexForConn(conn)
	return ch, done
}

func (s *codexWebsocketSession) activeCodexForConn(
	conn *websocket.Conn,
) (chan codexWebsocketRead, <-chan struct{}, *codexWebsocketTurnStats) {
	if s == nil || conn == nil {
		return nil, nil, nil
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeConn != conn {
		return nil, nil, nil
	}
	return s.activeCh, s.activeDone, s.activeTurn
}

func clearRetryActiveState(sess *codexWebsocketSession, conn *websocket.Conn, ch chan codexWebsocketRead) bool {
	if sess == nil {
		return false
	}
	return sess.clearActive(conn, ch)
}

func (s *codexWebsocketSession) clearActive(conn *websocket.Conn, ch chan codexWebsocketRead) bool {
	_, cleared := s.takeActive(conn, ch)
	return cleared
}

func (s *codexWebsocketSession) finishCodexTurn(
	conn *websocket.Conn,
	ch chan codexWebsocketRead,
	reason string,
	err error,
) bool {
	turn, cleared := s.takeActive(conn, ch)
	if !cleared || turn == nil {
		return cleared
	}
	fields := turn.fields(time.Now())
	fields["session"] = s.sessionID
	fields["reason"] = strings.TrimSpace(reason)
	if err != nil {
		fields["error_kind"] = codexWebsocketReadErrorKind(err)
	}
	log.WithFields(fields).Info("codex websocket upstream turn finished")
	return true
}

func (s *codexWebsocketSession) takeActive(
	conn *websocket.Conn,
	ch chan codexWebsocketRead,
) (*codexWebsocketTurnStats, bool) {
	if s == nil {
		return nil, false
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeConn != conn || s.activeCh != ch {
		return nil, false
	}
	turn := s.activeTurn
	s.activeConn = nil
	s.activeCh = nil
	s.activeTurn = nil
	if s.activeCancel != nil {
		s.activeCancel()
	}
	s.activeCancel = nil
	s.activeDone = nil
	return turn, true
}

func (s *codexWebsocketSession) writeMessage(conn *websocket.Conn, msgType int, payload []byte) error {
	if s == nil {
		return fmt.Errorf("codex websockets executor: session is nil")
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteMessage(msgType, payload)
}

// sendTerminalWebsocketRead reports whether it invalidated a full channel's connection before waiting.
func sendTerminalWebsocketRead(ch chan<- codexWebsocketRead, done <-chan struct{}, event codexWebsocketRead, invalidate func()) bool {
	select {
	case ch <- event:
		return false
	case <-done:
		return false
	default:
	}

	invalidated := invalidate != nil
	if invalidated {
		invalidate()
	}
	select {
	case ch <- event:
	case <-done:
	}
	return invalidated
}

func (s *codexWebsocketSession) configureConn(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.resetUpstreamDisconnectError(conn)
	conn.SetPingHandler(func(appData string) error {
		now := time.Now()
		fields := s.codexControlFields(conn, now, "ping", len(appData))
		errWrite := func() error {
			s.writeMu.Lock()
			defer s.writeMu.Unlock()
			// Reply pongs from the same write lock to avoid concurrent writes.
			return conn.WriteControl(websocket.PongMessage, []byte(appData), now.Add(10*time.Second))
		}()
		fields["pong_sent"] = errWrite == nil
		if errWrite != nil {
			log.WithFields(fields).Warn("codex websocket upstream control")
		} else {
			log.WithFields(fields).Info("codex websocket upstream control")
		}
		return errWrite
	})
	conn.SetPongHandler(func(appData string) error {
		fields := s.codexControlFields(conn, time.Now(), "pong", len(appData))
		log.WithFields(fields).Info("codex websocket upstream control")
		return nil
	})
	defaultCloseHandler := conn.CloseHandler()
	conn.SetCloseHandler(func(code int, text string) error {
		s.setUpstreamDisconnectError(conn, &websocket.CloseError{Code: code, Text: text})
		return defaultCloseHandler(code, text)
	})
}

func (s *codexWebsocketSession) codexControlFields(
	conn *websocket.Conn,
	now time.Time,
	control string,
	controlBytes int,
) log.Fields {
	fields := log.Fields{
		"session":       s.sessionID,
		"control":       control,
		"control_bytes": controlBytes,
		"observed_at":   now.Format(time.RFC3339Nano),
	}
	_, _, turn := s.activeCodexForConn(conn)
	for key, value := range turn.fields(now) {
		fields[key] = value
	}
	return fields
}

func (s *codexWebsocketSession) bindExecutionLifecycle(opts cliproxyexecutor.Options, conn *websocket.Conn, closer *websocketConnectionCloser, model string) error {
	if closer == nil {
		return fmt.Errorf("codex websockets executor: websocket connection closer is nil")
	}
	if s == nil {
		return cliproxyexecutor.BindExecutionResource(opts, closer)
	}
	lifecycle := opts.ExecutionLifecycle
	if lifecycle == nil || conn == nil {
		return nil
	}

	s.lifecycleBindMu.Lock()
	defer s.lifecycleBindMu.Unlock()

	s.connMu.Lock()
	if s.conn == conn && s.connCloser == nil {
		s.connCloser = closer
	}
	alreadyBound := s.conn == conn && s.connCloser == closer && s.lifecycle == lifecycle
	s.connMu.Unlock()
	if alreadyBound {
		return nil
	}

	if errBind := lifecycle.Bind(func() error {
		return s.closeBoundConnection(conn, closer, lifecycle)
	}); errBind != nil {
		return errBind
	}
	if retained, ok := lifecycle.(interface{ Retain() }); ok {
		retained.Retain()
	}

	s.connMu.Lock()
	if s.conn != conn || s.connCloser != closer {
		s.connMu.Unlock()
		return fmt.Errorf("codex websockets executor: websocket connection closed during lifecycle bind")
	}
	previous := s.lifecycle
	s.lifecycle = lifecycle
	s.lifecycleModel = strings.TrimSpace(model)
	s.connMu.Unlock()
	if previous != nil && previous != lifecycle {
		previous.End("target_replaced")
	}
	return nil
}

func (s *codexWebsocketSession) closeBoundConnection(conn *websocket.Conn, closer *websocketConnectionCloser, lifecycle cliproxyexecutor.ExecutionLifecycle) error {
	if s == nil || conn == nil {
		return nil
	}
	s.detachConnection(conn, lifecycle)
	errClose := closer.Close()
	go lifecycle.End("connection_closed")
	return errClose
}

func (s *codexWebsocketSession) detachConnection(conn *websocket.Conn, lifecycle cliproxyexecutor.ExecutionLifecycle) *websocketConnectionCloser {
	if s == nil || conn == nil {
		return nil
	}
	s.connMu.Lock()
	var closer *websocketConnectionCloser
	matched := s.conn == conn
	if matched {
		closer = s.connCloser
		s.conn = nil
		s.connCloser = nil
		if s.readerConn == conn {
			s.readerConn = nil
		}
	}
	if (lifecycle == nil && matched) || (lifecycle != nil && s.lifecycle == lifecycle) {
		s.lifecycle = nil
		s.lifecycleModel = ""
	}
	s.connMu.Unlock()
	return closer
}

func closeWebsocketAfterBindFailure(sess *codexWebsocketSession, conn *websocket.Conn, closer *websocketConnectionCloser) {
	if conn == nil || closer == nil {
		return
	}
	if sess != nil {
		sess.detachConnection(conn, nil)
	}
	if errClose := closer.Close(); errClose != nil {
		log.Errorf("websockets executor: close lifecycle bind failure connection error: %v", errClose)
	}
}

func websocketSessionTargetChanged(sess *codexWebsocketSession, authID string, wsURL string) bool {
	if sess == nil {
		return false
	}

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	if strings.TrimSpace(sess.authID) == "" && strings.TrimSpace(sess.wsURL) == "" {
		return false
	}
	return strings.TrimSpace(sess.authID) != strings.TrimSpace(authID) || strings.TrimSpace(sess.wsURL) != strings.TrimSpace(wsURL)
}

func existingWebsocketSessionConn(sess *codexWebsocketSession, authID string, wsURL string) (*websocket.Conn, *websocketConnectionCloser) {
	if sess == nil {
		return nil, nil
	}
	sess.connMu.Lock()
	conn := sess.conn
	closer := sess.connCloser
	matches := conn != nil && closer != nil &&
		strings.TrimSpace(sess.authID) == strings.TrimSpace(authID) &&
		strings.TrimSpace(sess.wsURL) == strings.TrimSpace(wsURL)
	sess.connMu.Unlock()
	if !matches || sess.upstreamDisconnectError(conn) != nil {
		return nil, nil
	}
	return conn, closer
}

func detachMismatchedWebsocketSessionConn(sess *codexWebsocketSession, authID string, wsURL string) (*websocket.Conn, *websocketConnectionCloser, string, string, cliproxyexecutor.ExecutionLifecycle) {
	if sess == nil {
		return nil, nil, "", "", nil
	}

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	conn := sess.conn
	if conn == nil || (strings.TrimSpace(sess.authID) == strings.TrimSpace(authID) && strings.TrimSpace(sess.wsURL) == strings.TrimSpace(wsURL)) {
		return nil, nil, "", "", nil
	}

	previousAuthID := sess.authID
	previousWSURL := sess.wsURL
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.connCloser = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	return conn, closer, previousAuthID, previousWSURL, lifecycle
}

func (s *codexWebsocketSession) resetUpstreamDisconnectError(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.upstreamDisconnectErrMu.Lock()
	s.upstreamDisconnectErrConn = conn
	s.upstreamDisconnectErr = nil
	s.upstreamDisconnectErrMu.Unlock()
}

func (s *codexWebsocketSession) setUpstreamDisconnectError(conn *websocket.Conn, err error) {
	if s == nil || conn == nil || err == nil {
		return
	}
	s.upstreamDisconnectErrMu.Lock()
	if s.upstreamDisconnectErrConn == conn && s.upstreamDisconnectErr == nil {
		s.upstreamDisconnectErr = err
	}
	s.upstreamDisconnectErrMu.Unlock()
}

func (s *codexWebsocketSession) upstreamDisconnectError(conn *websocket.Conn) error {
	if s == nil || conn == nil {
		return nil
	}
	s.upstreamDisconnectErrMu.RLock()
	defer s.upstreamDisconnectErrMu.RUnlock()
	if s.upstreamDisconnectErrConn != conn {
		return nil
	}
	return s.upstreamDisconnectErr
}

func (s *codexWebsocketSession) notifyUpstreamDisconnect(err error) {
	if s == nil {
		return
	}
	s.upstreamDisconnectOnce.Do(func() {
		if s.upstreamDisconnectCh == nil {
			return
		}
		select {
		case s.upstreamDisconnectCh <- err:
		default:
		}
		close(s.upstreamDisconnectCh)
	})
}

func executionSessionIDFromOptions(opts cliproxyexecutor.Options) string {
	if len(opts.Metadata) == 0 {
		return ""
	}
	raw, ok := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		return ""
	}
}

func (e *CodexWebsocketsExecutor) getOrCreateSession(sessionID string) *codexWebsocketSession {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	if e == nil {
		return nil
	}
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.sessions == nil {
		store.sessions = make(map[string]*codexWebsocketSession)
	}
	if sess, ok := store.sessions[sessionID]; ok && sess != nil {
		return sess
	}
	sess := &codexWebsocketSession{
		sessionID:            sessionID,
		upstreamDisconnectCh: make(chan error, 1),
	}
	store.sessions[sessionID] = sess
	return sess
}

func (e *CodexWebsocketsExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	sess := e.getOrCreateSession(sessionID)
	if sess == nil {
		return nil
	}
	return sess.upstreamDisconnectCh
}

func (e *CodexWebsocketsExecutor) ensureUpstreamConn(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, authID string, wsURL string, headers http.Header) (*websocket.Conn, *websocketConnectionCloser, *http.Response, error) {
	if sess == nil {
		return e.dialCodexWebsocket(ctx, auth, wsURL, headers)
	}

	if staleConn, staleCloser, staleAuthID, staleWSURL, staleLifecycle := detachMismatchedWebsocketSessionConn(sess, authID, wsURL); staleConn != nil {
		logCodexWebsocketDisconnected(sess.sessionID, staleAuthID, staleWSURL, "target_changed", nil)
		if staleCloser != nil {
			if errClose := staleCloser.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close stale websocket error: %v", errClose)
			}
		}
		if staleLifecycle != nil {
			staleLifecycle.End("target_changed")
		}
	}

	sess.connMu.Lock()
	conn := sess.conn
	closer := sess.connCloser
	readerConn := sess.readerConn
	sess.connMu.Unlock()
	if conn != nil {
		if readerConn != conn {
			sess.connMu.Lock()
			sess.readerConn = conn
			sess.connMu.Unlock()
			sess.configureConn(conn)
			go e.readUpstreamLoop(sess, conn)
		}
		return conn, closer, nil, nil
	}

	conn, closer, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
	if errDial != nil {
		return nil, closer, resp, errDial
	}

	sess.connMu.Lock()
	if sess.conn != nil {
		previous := sess.conn
		previousCloser := sess.connCloser
		sess.connMu.Unlock()
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		return previous, previousCloser, nil, nil
	}
	sess.conn = conn
	sess.connCloser = closer
	sess.wsURL = wsURL
	sess.authID = authID
	sess.readerConn = conn
	sess.connMu.Unlock()

	sess.configureConn(conn)
	go e.readUpstreamLoop(sess, conn)
	logCodexWebsocketConnected(sess.sessionID, authID, wsURL)
	return conn, closer, resp, nil
}

func (e *CodexWebsocketsExecutor) readUpstreamLoop(sess *codexWebsocketSession, conn *websocket.Conn) {
	if e == nil || sess == nil || conn == nil {
		return
	}
	stats := codexWebsocketReadStats{startedAt: time.Now()}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
		msgType, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			now := time.Now()
			ch, done, turn := sess.activeCodexForConn(conn)
			fields := stats.readStopFields(now, ch != nil, errRead)
			fields["session"] = sess.sessionID
			for key, value := range turn.fields(now) {
				fields[key] = value
			}
			if ch != nil && codexWebsocketReadErrorKind(errRead) != "local_close" {
				log.WithFields(fields).Warn("codex websocket upstream read stopped")
			} else {
				log.WithFields(fields).Info("codex websocket upstream read stopped")
			}
			invalidate := func() {
				e.invalidateUpstreamConn(sess, conn, "upstream_disconnected", errRead)
			}
			invalidated := false
			if ch != nil {
				invalidated = sendTerminalWebsocketRead(ch, done, codexWebsocketRead{conn: conn, err: errRead}, invalidate)
				if sess.clearActive(conn, ch) {
					close(ch)
				}
			}
			if !invalidated {
				invalidate()
			}
			return
		}

		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
				invalidate := func() {
					e.invalidateUpstreamConn(sess, conn, "unexpected_binary", errBinary)
				}
				invalidated := false
				ch, done := sess.activeForConn(conn)
				if ch != nil {
					invalidated = sendTerminalWebsocketRead(ch, done, codexWebsocketRead{conn: conn, err: errBinary}, invalidate)
					if sess.clearActive(conn, ch) {
						close(ch)
					}
				}
				if !invalidated {
					invalidate()
				}
				return
			}
			continue
		}

		ch, done, turn := sess.activeCodexForConn(conn)
		now := time.Now()
		fields, changed := stats.observeTextFrame(now, payload)
		logActivity := false
		if turn != nil {
			turnFields, turnChanged, turnActivity := turn.observeTextFrame(now, payload)
			for key, value := range turnFields {
				fields[key] = value
			}
			changed = turnChanged
			logActivity = turnActivity
		}
		if changed {
			fields["session"] = sess.sessionID
			fields["active_response"] = ch != nil
			log.WithFields(fields).Info("codex websocket upstream event")
		} else if logActivity {
			fields["session"] = sess.sessionID
			fields["active_response"] = ch != nil
			log.WithFields(fields).Info("codex websocket upstream activity")
		}
		if ch == nil {
			continue
		}
		select {
		case ch <- codexWebsocketRead{conn: conn, msgType: msgType, payload: payload}:
		case <-done:
		}
	}
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConn(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	e.invalidateUpstreamConnWithNotify(sess, conn, reason, err, true)
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConnWithoutDisconnectNotify(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	e.invalidateUpstreamConnWithNotify(sess, conn, reason, err, false)
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConnWithNotify(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error, notify bool) {
	if sess == nil || conn == nil {
		return
	}

	sess.connMu.Lock()
	current := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	sessionID := sess.sessionID
	if current == nil || current != conn {
		sess.connMu.Unlock()
		return
	}
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.connCloser = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sess.connMu.Unlock()

	logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, err)
	if notify {
		sess.notifyUpstreamDisconnect(err)
	}
	if closer != nil {
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
	}
	if lifecycle != nil {
		lifecycle.End(reason)
	}
}

func (e *CodexWebsocketsExecutor) CloseExecutionSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if e == nil {
		return
	}
	if sessionID == "" {
		return
	}
	if sessionID == cliproxyauth.CloseAllExecutionSessionsID {
		e.closeAllExecutionSessions("executor_shutdown")
		return
	}

	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sess := store.sessions[sessionID]
	delete(store.sessions, sessionID)
	store.mu.Unlock()

	e.closeExecutionSession(sess, "session_closed")
}

func (e *CodexWebsocketsExecutor) closeAllExecutionSessions(reason string) {
	if e == nil {
		return
	}

	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sessions := make([]*codexWebsocketSession, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		delete(store.sessions, sessionID)
		if sess != nil {
			sessions = append(sessions, sess)
		}
	}
	store.mu.Unlock()

	for i := range sessions {
		e.closeExecutionSession(sessions[i], reason)
	}
}

func (e *CodexWebsocketsExecutor) closeExecutionSession(sess *codexWebsocketSession, reason string) {
	closeCodexWebsocketSession(sess, reason)
}

func closeCodexWebsocketSession(sess *codexWebsocketSession, reason string) {
	if sess == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "session_closed"
	}

	sess.connMu.Lock()
	conn := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.connCloser = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sessionID := sess.sessionID
	sess.connMu.Unlock()

	if conn != nil {
		logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, nil)
		if closer != nil {
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}
	}
	if lifecycle != nil {
		lifecycle.End(reason)
	}
}

func logCodexWebsocketConnected(sessionID string, authID string, wsURL string) {
	log.Infof("codex websockets: upstream connected session=%s auth=%s url=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL))
}

func logCodexWebsocketDisconnected(sessionID string, authID string, wsURL string, reason string, err error) {
	if err != nil {
		log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s reason=%s err=%v", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason), err)
		return
	}
	log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s reason=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason))
}

// CloseCodexWebsocketSessionsForAuthID closes all active Codex upstream websocket sessions
// associated with the supplied auth ID.
func CloseCodexWebsocketSessionsForAuthID(authID string, reason string) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "auth_removed"
	}

	store := globalCodexWebsocketSessionStore
	if store == nil {
		return
	}

	type sessionItem struct {
		sessionID string
		sess      *codexWebsocketSession
	}

	store.mu.Lock()
	items := make([]sessionItem, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		items = append(items, sessionItem{sessionID: sessionID, sess: sess})
	}
	store.mu.Unlock()

	matches := make([]sessionItem, 0)
	for i := range items {
		sess := items[i].sess
		if sess == nil {
			continue
		}
		sess.connMu.Lock()
		sessAuthID := strings.TrimSpace(sess.authID)
		sess.connMu.Unlock()
		if sessAuthID == authID {
			matches = append(matches, items[i])
		}
	}
	if len(matches) == 0 {
		return
	}

	toClose := make([]*codexWebsocketSession, 0, len(matches))
	store.mu.Lock()
	for i := range matches {
		current, ok := store.sessions[matches[i].sessionID]
		if !ok || current == nil || current != matches[i].sess {
			continue
		}
		delete(store.sessions, matches[i].sessionID)
		toClose = append(toClose, current)
	}
	store.mu.Unlock()

	for i := range toClose {
		closeCodexWebsocketSession(toClose[i], reason)
	}
}
