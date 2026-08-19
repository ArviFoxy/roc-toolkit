Prometheus metrics
******************

Roc-toolkit can optionally expose internal metrics via a Prometheus HTTP endpoint.
This allows monitoring latency, jitter, packet loss, and other streaming statistics
in real time using Prometheus and Grafana.

Enabling
========

Prometheus support must be enabled at build time::

    scons --enable-prometheus

At runtime, pass ``--prometheus-metrics-port`` to ``roc-recv`` or ``roc-send``::

    roc-recv -s rtp://0.0.0.0:10001 --prometheus-metrics-port 9090
    roc-send -s rtp://192.168.1.10:10001 --prometheus-metrics-port 9091

Metrics are served at ``http://<host>:<port>/metrics``.

Labels
======

By default all metrics are exported without labels, one series per process.

A sender with multiple slots exports per-slot series when slots are given names
(``roc-send --slot-name``, or ``roc_slot_config.slot_name`` via the C API): the
name becomes the value of the ``slot`` label on all of that slot's metrics,
including the per-slot liveness gauge ``roc_send_slot_up`` (1 while the slot is
alive, 0 after it failed and detached). Without slot names, series of multiple
slots merge: counters sum and gauges are last-writer-wins.

Components shared between sender and receiver pipelines (latency tuner,
frequency estimators) export ``roc_send_*`` names on the sender and
``roc_recv_*`` names on the receiver.

FEC block histograms (``roc_recv_fec_block_missing``,
``roc_recv_fec_block_recovered``) carry a ``block_size`` label with one series
per observed FEC block size.

Histogram bucket configuration
==============================

Histogram metrics (latency, jitter, RTT) use logarithmically spaced buckets.
The bucket boundaries can be tuned via CLI flags:

.. list-table::
   :header-rows: 1
   :widths: 40 15 15

   * - Flag
     - Default
     - Description
   * - ``--prometheus-niq-latency-buckets``
     - 100
     - Number of histogram buckets for NIQ latency
   * - ``--prometheus-niq-latency-min``
     - 5ms
     - Minimum NIQ latency bucket boundary
   * - ``--prometheus-niq-latency-max``
     - 50ms
     - Maximum NIQ latency bucket boundary
   * - ``--prometheus-niq-latency-scale``
     - log
     - Bucket spacing: ``log`` or ``linear``
   * - ``--prometheus-e2e-latency-buckets``
     - 100
     - Number of histogram buckets for E2E latency
   * - ``--prometheus-e2e-latency-min``
     - 20ms
     - Minimum E2E latency bucket boundary
   * - ``--prometheus-e2e-latency-max``
     - 200ms
     - Maximum E2E latency bucket boundary
   * - ``--prometheus-e2e-latency-scale``
     - log
     - Bucket spacing: ``log`` or ``linear``
   * - ``--prometheus-jitter-buckets``
     - 100
     - Number of histogram buckets for jitter
   * - ``--prometheus-jitter-min``
     - 100us
     - Minimum jitter bucket boundary
   * - ``--prometheus-jitter-max``
     - 200ms
     - Maximum jitter bucket boundary
   * - ``--prometheus-jitter-scale``
     - log
     - Bucket spacing: ``log`` or ``linear``
   * - ``--prometheus-rtt-buckets``
     - 100
     - Number of histogram buckets for RTT
   * - ``--prometheus-rtt-min``
     - 1ms
     - Minimum RTT bucket boundary
   * - ``--prometheus-rtt-max``
     - 100ms
     - Maximum RTT bucket boundary
   * - ``--prometheus-rtt-scale``
     - log
     - Bucket spacing: ``log`` or ``linear``
   * - ``--prometheus-delay-deviation-buckets``
     - 90
     - Number of histogram buckets for delay deviation
   * - ``--prometheus-delay-deviation-min``
     - 1us
     - Minimum delay deviation bucket boundary
   * - ``--prometheus-delay-deviation-max``
     - 1s
     - Maximum delay deviation bucket boundary
   * - ``--prometheus-delay-deviation-scale``
     - log
     - Bucket spacing: ``log`` or ``linear``
   * - ``--prometheus-event-height-buckets``
     - 60
     - Number of histogram buckets for delay event height
   * - ``--prometheus-event-height-min``
     - 100us
     - Minimum delay event height bucket boundary
   * - ``--prometheus-event-height-max``
     - 1s
     - Maximum delay event height bucket boundary
   * - ``--prometheus-event-height-scale``
     - log
     - Bucket spacing: ``log`` or ``linear``
   * - ``--prometheus-queue-drain-buckets``
     - 40
     - Number of histogram buckets for queue drain
   * - ``--prometheus-queue-drain-min``
     - 100us
     - Minimum queue drain bucket boundary
   * - ``--prometheus-queue-drain-max``
     - 100ms
     - Maximum queue drain bucket boundary
   * - ``--prometheus-queue-drain-scale``
     - log
     - Bucket spacing: ``log`` or ``linear``

The delay deviation histogram resolves the BULK of the deviation
process - that is its purpose: the bulk shape tests the light-tail
assumption behind the event thresholds - so its default floor (1us)
sits below the diffusion noise. Event height starts at 100us: smaller
events consume no meaningful margin. Both cap at 1s, past the point
where a session restarts anyway. The event duration and gap
histograms use fixed log buckets (1ms to 10s and 100ms to 1000s) and
have no flags yet.

Receiver metrics
================

FreqEstimator
-------------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_freq_estimator_coeff``
     - Gauge
     - Frequency compensation coefficient (fluctuates around 1.0)
   * - ``roc_recv_freq_estimator_stable``
     - Gauge
     - Whether the estimator has converged (0 or 1)

Watchdog
--------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_session_restarts_total{cause=...}``
     - Counter
     - Session restarts by cause: ``no_playback_timeout`` (every frame blank
       for the whole no-play timeout), ``choppy_playback_timeout`` (packet
       drops in every window for the whole choppy-play timeout),
       ``latency_out_of_tolerance`` (latency left target +/- tolerance;
       incremented by the latency tuner, not the watchdog)

FEC BlockReader
---------------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_fec_block_missing``
     - Histogram
     - Distribution of missing source packets per FEC block (before + after repair)
   * - ``roc_recv_fec_block_recovered``
     - Histogram
     - Distribution of FEC-recovered source packets per FEC block

Buckets are auto-sized to the FEC block length (one bucket per integer: 0, 1, 2, ..., N).
No CLI configuration needed.

LinkMeter
---------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_packets_expected_total``
     - Counter
     - Total expected packets based on sequence numbers
   * - ``roc_recv_packets_lost_total``
     - Counter
     - Total packets never received (wire loss)
   * - ``roc_recv_packets_received_total``
     - Counter
     - Total packets received
   * - ``roc_recv_clock_drift_ppm``
     - Gauge
     - Estimated clock drift between sender and receiver in ppm, from RTP timestamps
   * - ``roc_recv_clock_drift_stddev_ppm``
     - Gauge
     - Standard deviation of clock drift estimate in ppm

JitterMeter
-----------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_jitter_seconds``
     - Histogram
     - Distribution of instantaneous inter-packet jitter
   * - ``roc_recv_jitter_envelope_seconds``
     - Gauge
     - Smoothed jitter spike envelope (capacitor model)
   * - ``roc_recv_jitter_mean_seconds``
     - Gauge
     - Moving window average of recent jitter

Delay process
-------------

The ``jitter_*`` metrics above are the absolute FIRST DIFFERENCE of the
arrival process (change of delay between consecutive packets, RFC 3550
style). They indicate link quality, but the delay LEVEL's behavior is
not recoverable from them: a slow queue drain is invisible, oscillation
is double-counted, and the recovery flush after a pause is counted as
jitter. The ``delay_*`` family below measures the LEVEL itself: per
packet, arrival time minus schedule time on the RTP timeline.

The level is CLOCK-FREE: it compares the local receive clock with the
RTP packet spacing. The unknown constant (network transit plus clock
offset) cancels against the baseline, so no clock synchronization
between hosts is assumed for this family.

``roc_recv_queue_drain_seconds`` is the model-free validator: per
snapshot grid interval, the mean minus the minimum of the queue depth.
The event statistics (rate, height distribution, gap structure) must
predict its distribution, or the delay model is wrong.

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_delay_deviation_seconds``
     - Histogram
     - Per-packet delay deviation (level minus baseline, clamped at zero); the bulk shape tests the light-tail assumption behind the event thresholds
   * - ``roc_recv_delay_event_height_seconds``
     - Histogram
     - Peak deviation per detected event = buffer margin the event consumed
   * - ``roc_recv_delay_event_duration_seconds``
     - Histogram
     - Time per event above the close threshold; height ~ duration is the pause-shape prediction
   * - ``roc_recv_delay_event_gap_seconds``
     - Histogram
     - Time from one event's close to the next event's open; exponential gaps mean Poisson events, excess short gaps mean clustering
   * - ``roc_recv_delay_baseline_seconds``
     - Gauge
     - Exponential mean of the delay level (constant part arbitrary; only changes meaningful)
   * - ``roc_recv_delay_floor_seconds``
     - Gauge
     - Rolling minimum of the delay level; baseline minus floor = standing latency cost of the jitter bulk
   * - ``roc_recv_delay_bulk_stddev_seconds``
     - Gauge
     - Exponential stddev of the SIGNED deviation outside events (frozen while an event is open); the signed second moment measures the symmetric bulk at its full scale, which is what justifies the 6 sigma false-event rate
   * - ``roc_recv_delay_deviation_max_seconds``
     - Gauge
     - Rolling maximum of the deviation (diagnostic)
   * - ``roc_recv_delay_event_total``
     - Counter
     - Detected delay events (6/3 sigma hysteresis; false-event rate ~1e-9 per packet for light-tailed bulk)
   * - ``roc_recv_queue_drain_seconds``
     - Histogram
     - Queue drain depth per grid interval (mean minus min of queue depth); the model-free validator

Depacketizer
------------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_samples_decoded_total``
     - Counter
     - Total audio samples decoded from packets
   * - ``roc_recv_samples_missing_total``
     - Counter
     - Total samples missing due to lost packets
   * - ``roc_recv_samples_late_total``
     - Counter
     - Total samples dropped because packet arrived after playout
   * - ``roc_recv_packets_decoded_total``
     - Counter
     - Total packets decoded
   * - ``roc_recv_packets_late_total``
     - Counter
     - Total packets arrived after playout deadline
   * - ``roc_recv_fec_recovered_packets_total``
     - Counter
     - Total packets recovered via FEC
   * - ``roc_recv_fec_recovered_samples_total``
     - Counter
     - Total samples recovered via FEC

LatencyTuner
------------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_latency_target_seconds``
     - Gauge
     - Current target latency (changes in adaptive mode)
   * - ``roc_recv_latency_seconds``
     - Histogram
     - Network input queue (NIQ) latency distribution

PreciseFreqEstimator
--------------------

These metrics are only active when ``--latency-profile=precise`` is used.
The ``roc_recv_freq_estimator_coeff`` and ``roc_recv_freq_estimator_stable``
metrics above are also emitted by this estimator with the same semantics.

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_freq_estimator_correction``
     - Gauge
     - Proportional buffer correction component (K × error)
   * - ``roc_recv_freq_estimator_correction_rms``
     - Gauge
     - Smoothed RMS of proportional correction

LatencyMonitor
--------------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_recv_e2e_latency_seconds``
     - Histogram
     - End-to-end latency distribution (capture to playback)
   * - ``roc_recv_niq_stalling_seconds``
     - Gauge
     - Time since last received packet (resets on arrival)
   * - ``roc_recv_fec_block_duration_seconds``
     - Gauge
     - Duration of one FEC block

Sender metrics
==============

Packetizer
----------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_send_packets_encoded_total``
     - Counter
     - Total packets encoded and sent
   * - ``roc_send_payload_bytes_total``
     - Counter
     - Total payload bytes sent

FeedbackMonitor
---------------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_send_e2e_latency_seconds``
     - Histogram
     - End-to-end latency reported by receiver via RTCP
   * - ``roc_send_niq_latency_seconds``
     - Histogram
     - NIQ latency reported by receiver via RTCP
   * - ``roc_send_packets_lost_total``
     - Counter
     - Total packets reported lost by receiver via RTCP
   * - ``roc_send_jitter_mean_seconds``
     - Gauge
     - Mean jitter reported by receiver via RTCP
   * - ``roc_send_rtt_seconds``
     - Histogram
     - Round-trip time distribution from RTCP timestamp exchange

Session sync metrics (report plane)
===================================

When receivers run with a non-zero ``--report-grid`` (roc-recv default
500ms; the library default is off), they sample telemetry snapshots each
time playback crosses a grid point on the sender clock timeline and
report them via the non-standard XR Stream Snapshot block (BT=221). The
session sender's skew estimator turns them into cross-receiver
statistics, exported per slot (``slot=`` label, one series per slot),
per pair (``slot_a``/``slot_b``), and fleet-wide.

The sync statistics (offsets, skew matrix, fleet mean/stddev/spread,
jump detector) derive from e2e latency at a common stream position:
capture timestamp to projected playback on the receiver clock, the span
listeners hear, including sender batching, network transit and sink
projection. The sync metrics assume all hosts' clocks are NTP/chrony-
synchronized to microsecond-level error; e2e latency inherits clock
error, so under this assumption e2e differences between receivers equal
playout-timing differences. A slot whose snapshot carries no e2e value
(no RTCP clock mapping yet) is absent from that row's sync statistics.
Queue depth (the arrival-to-read span) excludes sender batching, LAN
transit and sink projection; it measures buffer margin (distance to
underrun) and feeds only the correlation matrix and per-slot RMS, a
transport diagnostic.

SessionSkewEstimator
--------------------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_send_playout_offset_seconds{slot=...}``
     - Gauge
     - E2E playout offset vs fleet median
   * - ``roc_send_playout_offset_rms_seconds{slot=...}``
     - Gauge
     - EWMA RMS of queue-depth fluctuations (15min time constant)
   * - ``roc_send_recv_warp{slot=...}``
     - Gauge
     - Receiver-reported warp (frequency coefficient - 1)
   * - ``roc_send_recv_target_latency_seconds{slot=...}``
     - Gauge
     - Receiver-reported target latency
   * - ``roc_send_snapshot_timestamp_seconds{slot=...}``
     - Gauge
     - Unix time of the last ACCEPTED snapshot; age = time() - value
   * - ``roc_send_offset_jump_active{slot=...}``
     - Gauge
     - 1 while an offset jump event is in progress
   * - ``roc_send_offset_jump_magnitude_seconds{slot=...}``
     - Gauge
     - Peak offset excursion of the last jump event
   * - ``roc_send_offset_jump_duration_seconds{slot=...}``
     - Gauge
     - Duration of the last completed jump event
   * - ``roc_send_offset_jump_total{slot=...}``
     - Counter
     - Offset jump events per slot (2ms step or 10ms absolute trigger)
   * - ``roc_send_playout_skew_seconds{slot_a=...,slot_b=...}``
     - Gauge
     - E2E playout skew between two slots
   * - ``roc_send_playout_corr{slot_a=...,slot_b=...}``
     - Gauge
     - Correlation of the two slots' queue-depth (buffer margin) fluctuations, a transport diagnostic (exponential averages, 15min time constant)
   * - ``roc_send_playout_cov_seconds2{slot_a=...,slot_b=...}``
     - Gauge
     - Covariance of the two slots' queue-depth fluctuations (exponential averages, 15min time constant)
   * - ``roc_send_recv_deviation_mean_seconds{slot=...}``
     - Gauge
     - Receiver-reported mean delay deviation over the last snapshot interval
   * - ``roc_send_recv_deviation_max_seconds{slot=...}``
     - Gauge
     - Receiver-reported maximum delay deviation over the last snapshot interval
   * - ``roc_send_recv_event_count{slot=...}``
     - Gauge
     - Receiver-reported delay events closed in the last snapshot interval
   * - ``roc_send_event_rows_total{slot=...}``
     - Counter
     - Snapshot rows in which the slot reported at least one delay event
   * - ``roc_send_joint_event_rows_total{slot_a=...,slot_b=...}``
     - Counter
     - Snapshot rows in which both slots reported delay events
   * - ``roc_send_event_dependence_ratio{slot_a=...,slot_b=...}``
     - Gauge
     - Joint event rows times total rows over the product of the two slots' event-row counts; 1 = independent, above 1 = shared events
   * - ``roc_send_deviation_corr{slot_a=...,slot_b=...}``
     - Gauge
     - Correlation of the two slots' mean delay deviations (exponential averages)
   * - ``roc_send_deviation_cov_seconds2{slot_a=...,slot_b=...}``
     - Gauge
     - Covariance of the two slots' mean delay deviations (exponential averages)
   * - ``roc_send_playout_fleet_mean_seconds``
     - Gauge
     - Mean e2e latency across the fleet at a common stream position
   * - ``roc_send_playout_fleet_mean``
     - Histogram
     - Distribution of the fleet mean over time (one sample per grid row)
   * - ``roc_send_playout_stddev_seconds``
     - Gauge
     - Population stddev of e2e latency across the fleet at a common stream position
   * - ``roc_send_playout_stddev``
     - Histogram
     - Distribution of the fleet stddev over time (one sample per grid row)
   * - ``roc_send_playout_spread_seconds``
     - Gauge
     - Max-min e2e latency across the fleet
   * - ``roc_send_playout_spread``
     - Histogram
     - Distribution of fleet spread over time (one sample per grid row)
   * - ``roc_send_snapshot_rows_total{completeness="full|partial"}``
     - Counter
     - Finalized snapshot grid rows
   * - ``roc_send_snapshot_rejected_total``
     - Counter
     - Snapshots rejected by the validity gates

The stddev histogram shares the ``--prometheus-playout-spread-*`` bucket
bounds (default log, 10us to 10ms); the fleet mean histogram has its own
``--prometheus-playout-fleet-mean-*`` flags (default log, 5ms to 100ms).
Defaults put the log midpoint near each statistic's observed mode.

Tail dependence is read directly off the event-row counters. If the
slots' pause processes are independent Poisson processes, the joint
rate equals the product of the marginal event-row rates (per row).
An excess of ``joint_event_rows`` over that product means shared
pauses: a common upstream cause (sender host, shared network segment)
rather than receiver-local ones. The corresponding underrun link: with
margin ``m``, event rate ``lambda`` and height distribution ``F``,
``P(underrun by time T) = 1 - exp(-lambda * T * (1 - F(m)))`` under
the Poisson null; the gap histogram measures the deviation from that
null.

Mixed versions are safe in both directions. The snapshot block
carries its on-wire entry size: an old sender reads the seven-word
prefix of a new receiver's larger entries by striding, and a new
sender accepts an old receiver's seven-word entries with the delay
fields reported unavailable (the sender-side delay gauges and
counters for that slot simply stay silent). No deploy ordering is
required.

IO metrics
==========

These metrics are exported by the PulseAudio/PipeWire audio backend
and apply to both sender and receiver. The ``device`` label distinguishes
playback (``sink``) from capture (``source``).

PulseaudioDevice
----------------

.. list-table::
   :header-rows: 1
   :widths: 45 12 43

   * - Metric
     - Type
     - Description
   * - ``roc_io_latency_seconds{device="sink|source"}``
     - Gauge
     - Actual IO buffer latency reported by the audio backend
   * - ``roc_io_target_latency_seconds{device="sink|source"}``
     - Gauge
     - Requested IO buffer latency (set at startup)
   * - ``roc_io_stream_restarts_total{device="sink|source"}``
     - Counter
     - Number of times the audio backend stream was restarted due to errors
