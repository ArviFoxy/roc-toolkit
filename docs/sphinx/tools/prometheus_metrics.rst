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
   * - ``roc_recv_session_restarts_total``
     - Counter
     - Number of times a session was restarted due to timeout

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

The clock-free offsets derive from queue depths at a common stream
position: emission is common (one sender clock), so receiver i plays a
position at approximately arrival + queue_i, and differences of queue
depths at the same position measure playout skew without any wall-clock
agreement. The e2e-based offsets inherit the NTP error of the RTCP clock
mapping. The per-slot difference of the two contains the differential
mapping error TOGETHER WITH the constant per-receiver device buffering:
the clock-free offset measures the decode point, while device buffering
shows only in the e2e view. Treat the disagreement as an upper bound on
the mapping error, not as the mapping error itself.

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
     - Clock-free playout offset vs fleet median
   * - ``roc_send_playout_offset_e2e_seconds{slot=...}``
     - Gauge
     - E2E-based playout offset vs fleet median (inherits NTP error)
   * - ``roc_send_playout_offset_disagreement_seconds{slot=...}``
     - Gauge
     - Clock-free minus e2e offset (mapping error plus device buffering)
   * - ``roc_send_playout_offset_rms_seconds{slot=...}``
     - Gauge
     - EWMA RMS of queue-depth changes (60s time constant)
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
     - Clock-free playout skew between two slots
   * - ``roc_send_playout_corr{slot_a=...,slot_b=...}``
     - Gauge
     - Correlation of the two slots' latencies (exponential averages, 15min time constant)
   * - ``roc_send_playout_cov_seconds2{slot_a=...,slot_b=...}``
     - Gauge
     - Covariance of the two slots' latencies (exponential averages, 15min time constant)
   * - ``roc_send_playout_fleet_mean_seconds``
     - Gauge
     - Mean queue depth across the fleet at a common stream position
   * - ``roc_send_playout_fleet_mean``
     - Histogram
     - Distribution of the fleet mean over time (one sample per grid row)
   * - ``roc_send_playout_stddev_seconds``
     - Gauge
     - Population stddev of queue depth across the fleet at a common stream position
   * - ``roc_send_playout_stddev``
     - Histogram
     - Distribution of the fleet stddev over time (one sample per grid row)
   * - ``roc_send_playout_spread_seconds``
     - Gauge
     - Max-min clock-free skew across the fleet
   * - ``roc_send_playout_spread``
     - Histogram
     - Distribution of fleet spread over time (one sample per grid row)
   * - ``roc_send_playout_common_mode_seconds``
     - Gauge
     - Fleet mean queue depth minus its slow baseline
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

Mixed versions are safe in both directions: an old sender skips the
unknown XR block, and an old receiver simply never sends it (the
sender's snapshot timestamps go stale, which is itself visible).

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
