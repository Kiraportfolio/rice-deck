/* Shared transport between the deck and the phone remote.
   Three channels run side by side; whichever is alive delivers the message.
     local — other tabs in the same browser (localStorage)
     lan   — same Wi-Fi, via server.js (SSE down, POST up)
     cloud — opt-in public MQTT broker, for when the LAN blocks device-to-device
*/
(function (global) {
    'use strict';

    var LS_KEY = 'rice_sync_msg';
    var ROOM_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
    var MQTT_LIB = 'https://cdn.jsdelivr.net/npm/mqtt@5.10.1/dist/mqtt.min.js';
    var MQTT_URL = 'wss://broker.emqx.io:8084/mqtt';
    var TOPIC_PREFIX = 'thai-rice-deck/';

    function randomRoom() {
        var s = '';
        for (var i = 0; i < 6; i++) s += ROOM_ALPHABET[Math.floor(Math.random() * ROOM_ALPHABET.length)];
        return s;
    }

    function loadScript(src) {
        return new Promise(function (resolve, reject) {
            var s = document.createElement('script');
            s.src = src;
            s.onload = resolve;
            s.onerror = function () { reject(new Error('load failed')); };
            document.head.appendChild(s);
        });
    }

    function create(opts) {
        var role = opts.role || 'peer';
        var onMessage = opts.onMessage || function () { };
        var onStatus = opts.onStatus || function () { };
        var id = role + '-' + Math.random().toString(36).slice(2, 9);
        var room = opts.room || null;

        var state = { lan: false, cloud: false, cloudPending: false };

        function pushStatus() {
            onStatus({ lan: state.lan, cloud: state.cloud, cloudPending: state.cloudPending, room: room, id: id });
        }

        function receive(msg) {
            if (!msg || msg.from === id) return;
            onMessage(msg);
        }

        /* ── local: other tabs, same browser ── */
        global.addEventListener('storage', function (e) {
            if (e.key !== LS_KEY || !e.newValue) return;
            try { receive(JSON.parse(e.newValue)); } catch (_) { }
        });

        function sendLocal(msg) {
            try { localStorage.setItem(LS_KEY, JSON.stringify(msg)); } catch (_) { }
        }

        /* ── lan: server.js over the same Wi-Fi ── */
        var es = null;

        function startLan() {
            if (!/^https?:$/.test(location.protocol)) return;
            fetch('/api/state', { cache: 'no-store' })
                .then(function (r) { if (!r.ok) throw new Error('no server'); return r.json(); })
                .then(function () {
                    es = new EventSource('/api/events?id=' + encodeURIComponent(id) + '&role=' + role);
                    es.onopen = function () { state.lan = true; pushStatus(); };
                    es.onerror = function () { state.lan = false; pushStatus(); };
                    es.onmessage = function (ev) {
                        try { receive(JSON.parse(ev.data)); } catch (_) { }
                    };
                })
                .catch(function () { state.lan = false; pushStatus(); });
        }

        function sendLan(msg) {
            if (!state.lan) return;
            fetch('/api/cmd', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(msg),
                keepalive: true
            }).catch(function () { });
        }

        /* ── cloud: opt-in public broker ── */
        var client = null;

        function enableCloud(code) {
            room = code || room || randomRoom();
            if (state.cloudPending || state.cloud) return Promise.resolve(room);
            state.cloudPending = true;
            pushStatus();

            return Promise.resolve(global.mqtt ? null : loadScript(MQTT_LIB))
                .then(function () {
                    if (client) { try { client.end(true); } catch (_) { } }
                    client = global.mqtt.connect(MQTT_URL, {
                        clientId: 'rice_' + id + '_' + Date.now().toString(36),
                        reconnectPeriod: 4000,
                        connectTimeout: 10000,
                        clean: true
                    });
                    client.on('connect', function () {
                        client.subscribe(TOPIC_PREFIX + room);
                        state.cloud = true;
                        state.cloudPending = false;
                        pushStatus();
                    });
                    client.on('message', function (_topic, payload) {
                        try { receive(JSON.parse(payload.toString())); } catch (_) { }
                    });
                    client.on('close', function () { state.cloud = false; pushStatus(); });
                    client.on('error', function () { state.cloud = false; state.cloudPending = false; pushStatus(); });
                    return room;
                })
                .catch(function () {
                    state.cloud = false;
                    state.cloudPending = false;
                    pushStatus();
                    throw new Error('cloud unavailable');
                });
        }

        function disableCloud() {
            if (client) { try { client.end(true); } catch (_) { } client = null; }
            state.cloud = false;
            state.cloudPending = false;
            pushStatus();
        }

        function sendCloud(msg) {
            if (!client || !state.cloud) return;
            try { client.publish(TOPIC_PREFIX + room, JSON.stringify(msg)); } catch (_) { }
        }

        startLan();
        pushStatus();

        return {
            id: id,
            get room() { return room; },
            get status() { return { lan: state.lan, cloud: state.cloud, room: room }; },
            get connected() { return state.lan || state.cloud; },
            enableCloud: enableCloud,
            disableCloud: disableCloud,
            send: function (msg) {
                var full = Object.assign({}, msg, { from: id, role: role, ts: Date.now() });
                sendLocal(full);
                sendLan(full);
                sendCloud(full);
            }
        };
    }

    global.DeckSync = { create: create, randomRoom: randomRoom };
})(window);
