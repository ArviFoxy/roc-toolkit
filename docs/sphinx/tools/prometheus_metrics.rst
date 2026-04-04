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
   * - ``--prometheus-latency-buckets``
     - 32
     - Number of histogram buckets for latency
   * - ``--prometheus-latency-min``
     - 1ms
     - Minimum latency bucket boundary
   * - ``--prometheus-latency-max``
     - 1s
     - Maximum latency bucket boundary
   * - ``--prometheus-jitter-buckets``
     - 32
     - Number of histogram buckets for jitter
   * - ``--prometheus-jitter-min``
     - 100us
     - Minimum jitter bucket boundary
   * - ``--prometheus-jitter-max``
     - 200ms
     - Maximum jitter bucket boundary
   * - ``--prometheus-rtt-buckets``
     - 32
     - Number of histogram buckets for RTT
   * - ``--prometheus-rtt-min``
     - 100us
     - Minimum RTT bucket boundary
   * - ``--prometheus-rtt-max``
     - 200ms
     - Maximum RTT bucket boundary

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
