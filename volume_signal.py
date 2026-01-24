def volume_acceleration_time(ring, short_sec=15, long_sec=180,
                            accel_thresh=2.2, min_now=20.0, min_base=5.0,
                            accel_cap=50.0):
    if ring.span_sec() < (short_sec + long_sec) * 0.6:
        return False, 1.0, 0.0, 0.0

    t_end = ring.d[-1][0]

    recent_pts = [(t, v) for t, v in ring.d if t >= (t_end - short_sec)]
    base_pts   = [(t, v) for t, v in ring.d if (t_end - short_sec - long_sec) <= t < (t_end - short_sec)]

    if len(recent_pts) < 3 or len(base_pts) < 5:
        return False, 1.0, 0.0, 0.0

    recent_span = max(recent_pts[-1][0] - recent_pts[0][0], 1e-6)
    base_span   = max(base_pts[-1][0] - base_pts[0][0], 1e-6)

    v_now  = sum(v for _, v in recent_pts) / recent_span
    v_base = sum(v for _, v in base_pts)   / base_span

    if v_now < min_now or v_base < min_base:
        return False, 1.0, float(v_now), float(v_base)

    accel = v_now / (v_base + 1e-9)
    if accel > accel_cap:
        accel = accel_cap

    return (accel >= accel_thresh), float(accel), float(v_now), float(v_base)







