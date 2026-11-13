
### memory: throughput, messages/s, median (lowest–highest round)

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| warmup send | 30,537 (29,393–32,869) | 18,147 (18,091–18,194) | 24,023 (22,986–24,517) |
| warmup consume | 65,397 (63,213–77,632) | 38,632 (37,428–39,046) | 53,590 (53,407–54,097) |
| standard-1conn send | 13,412 (12,347–13,675) | 3,072 (2,689–3,073) | 3,705 (3,343–3,899) |
| standard-1conn consume | 31,611 (29,022–33,135) | 10,024 (9,486–10,543) | 13,104 (12,048–13,141) |
| standard-8conn send | 30,702 (30,224–32,332) | 20,844 (20,615–21,468) | 30,253 (29,708–30,290) |
| standard-8conn consume | 75,899 (67,044–80,533) | 50,245 (46,650–50,972) | 62,877 (62,182–63,787) |
| standard-64conn send | 37,037 (36,877–39,900) | 30,878 (29,806–31,680) | 54,515 (54,383–54,767) |
| standard-64conn consume | 75,854 (72,293–82,741) | 58,197 (51,031–58,921) | 73,449 (72,522–78,039) |
| fifo-8conn send | 5,640 (5,553–5,695) | 5,421 (5,332–5,488) | 7,191 (7,126–7,198) |
| fifo-8conn consume | 57,569 (52,276–62,445) | 5,033 (4,956–5,427) | 8,500 (8,409–8,533) |
| 10queues-64conn send | 38,831 (37,680–39,321) | 30,895 (30,567–31,388) | 54,619 (54,537–54,644) |
| 10queues-64conn consume | 81,356 (71,030–82,973) | 60,451 (60,352–63,283) | 76,039 (75,604–82,943) |
| body-256KiB send | 3,527 (2,216–3,574) | 541 (533–546) | 1,338 (1,304–1,371) |
| body-256KiB consume | 2,530 (2,033–2,560) | 1,228 (1,175–1,306) | 1,917 (1,841–1,966) |
| body-5MiB send | 173 (123–200) | 39.2 (39.0–44.7) | 71.2 (62.4–86.3) |
| body-5MiB consume | 102 (98.6–128) | 54.0 (52.9–54.6) | 59.7 (0.00–70.2) |
| 100k backlog fill, batches of 10 | 83,464 (83,070–84,174) | 51,618 (46,603–53,914) | 71,060 (70,105–71,268) |
| Consume from 100k backlog | 66,420 (56,654–78,948) | 47,543 (20,112–55,602) | 65,311 (64,085–71,632) |

### memory: latency at fixed rates

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| rate-200 achieved msg/s | 200 (200–200) | 200 (200–200) | 200 (200–200) |
| rate-200 send p50 ms | 2.31 (2.28–2.38) | 2.45 (2.42–2.48) | 2.44 (2.41–2.48) |
| rate-200 send p99 ms | 4.74 (4.48–7.13) | 4.71 (3.92–4.95) | 4.60 (4.37–4.70) |
| rate-200 send p99.9 ms | 10.9 (7.42–22.7) | 16.6 (12.3–45.8) | 6.82 (6.31–7.49) |
| rate-200 end-to-end p50 ms | 2.27 (2.08–2.33) | 2.47 (2.12–2.49) | 2.45 (2.41–2.49) |
| rate-200 end-to-end p99 ms | 4.60 (4.45–7.16) | 4.47 (3.93–5.00) | 4.62 (4.42–4.72) |
| rate-1000 achieved msg/s | 1000 (1000–1000) | 1000 (1000–1000) | 1000 (1000–1000) |
| rate-1000 send p50 ms | 1.56 (1.54–1.63) | 1.46 (1.41–1.48) | 1.42 (1.35–1.43) |
| rate-1000 send p99 ms | 3.67 (3.08–12.3) | 2.47 (2.45–2.52) | 2.46 (2.37–2.56) |
| rate-1000 send p99.9 ms | 7.67 (5.18–24.2) | 7.09 (6.48–27.4) | 7.71 (5.31–12.9) |
| rate-1000 end-to-end p50 ms | 1.55 (1.23–1.60) | 1.48 (1.41–1.57) | 1.43 (1.41–1.48) |
| rate-1000 end-to-end p99 ms | 3.73 (2.71–13.6) | 2.49 (2.47–2.62) | 2.54 (2.48–2.54) |
| rate-5000 achieved msg/s | 4,999 (4,999–4,999) | 4,999 (4,999–4,999) | 4,999 (4,999–4,999) |
| rate-5000 send p50 ms | 1.32 (1.20–1.33) | 1.17 (1.05–1.18) | 1.17 (1.08–1.22) |
| rate-5000 send p99 ms | 2.79 (2.68–5.27) | 11.5 (10.1–11.7) | 2.58 (2.49–2.68) |
| rate-5000 send p99.9 ms | 6.80 (3.44–19.1) | 29.6 (15.8–38.2) | 14.2 (13.9–14.7) |
| rate-5000 end-to-end p50 ms | 1.44 (1.31–1.44) | 1.32 (1.19–1.63) | 1.37 (1.35–1.40) |
| rate-5000 end-to-end p99 ms | 3.03 (2.88–6.27) | 13.1 (11.6–13.3) | 3.19 (3.16–3.23) |

### memory: resources

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| Startup to first healthy response, ms | 54.7 (54.3–66.9) | 91.8 (90.7–126) | 1,084 (1,042–1,175) |
| Idle process memory (anon), MiB | 0.17 (0.16–0.18) | 14.8 (14.8–14.9) | 152 (151–155) |
| Idle cgroup usage (with page cache), MiB | 1.03 (0.81–1.27) | 16.8 (16.1–21.4) | 154 (153–204) |
| Peak memory, standard-64conn send, MiB | 233 (232–251) | 513 (348–581) | 1,208 (1,203–1,219) |
| Server CPU seconds per 1,000 sends, 8 conns | 0.07 (0.06–0.07) | 0.15 (0.15–0.15) | 0.11 (0.11–0.11) |
| Server CPU seconds per 1,000 receive+delete, 8 conns | 0.02 (0.02–0.02) | 0.06 (0.06–0.06) | 0.05 (0.05–0.05) |
| Server cores used, 64-conn send | 2.39 (2.35–2.39) | 3.33 (3.16–3.39) | 3.82 (3.81–3.84) |
| Load generator cores used, 64-conn send (limit 4) | 1.38 (1.33–1.52) | 0.95 (0.89–0.97) | 1.76 (1.74–1.82) |
| Memory with 1,000 long polls, MiB | 3.93 (3.77–4.17) | 676 (647–833) | 1,242 (1,227–1,258) |
| Server cores with 1,000 long polls | 0.02 (0.02–0.02) | 0.09 (0.08–0.09) | 0.12 (0.12–0.12) |
| Memory holding 100k backlog, MiB | 107 (107–107) | 899 (790–1,100) | 1,243 (1,228–1,262) |
| Memory growth per backlog message, bytes | 1,080 (1,079–1,080) | 2,341 (1,505–2,767) | 13.7 (12.2–43.3) |
| Disk with 100k backlog, MiB | – | – | – |
| Restart with backlog: startup ms | – | – | – |
| Restart with backlog: restored | – | – | – |
| Restart with backlog: expected | – | – | – |
| SIGKILL: acknowledged sends | – | – | – |
| SIGKILL: acknowledged sends lost | – | – | – |
| 1 GiB limit: idle memory, MiB | 0.16 (0.16–0.17) | 14.8 (14.8–14.9) | 104 (104–106) |
| 1 GiB limit: send, msg/s | 32,218 (30,668–32,341) | 17,767 (17,545–17,964) | 19,618 (19,379–19,736) |
| 1 GiB limit: fill 50k, msg/s | 82,907 (82,649–86,353) | 59,600 (58,113–59,606) | 53,947 (53,837–54,785) |
| 1 GiB limit: memory after 50k, MiB | 54.3 (54.2–54.3) | 210 (111–238) | 417 (416–424) |

### durable: throughput, messages/s, median (lowest–highest round)

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| warmup send | 14,686 (12,431–14,723) | 11,370 (11,368–11,825) | 15,722 (14,931–15,879) |
| warmup consume | 34,795 (24,407–34,803) | 9,393 (9,220–9,727) | 16,369 (16,267–16,653) |
| standard-1conn send | 3,711 (2,878–3,835) | 2,570 (2,554–2,572) | 3,231 (2,885–3,250) |
| standard-1conn consume | 11,559 (10,008–11,686) | 5,456 (5,228–5,486) | 7,871 (7,434–8,038) |
| standard-8conn send | 14,354 (11,743–14,829) | 14,689 (14,521–15,031) | 21,193 (21,125–21,469) |
| standard-8conn consume | 35,296 (29,794–35,969) | 10,775 (10,670–11,096) | 17,285 (16,882–17,318) |
| standard-64conn send | 33,339 (32,812–34,194) | 17,932 (17,570–18,478) | 34,135 (33,011–34,687) |
| standard-64conn consume | 56,154 (52,140–59,063) | 10,334 (9,750–10,612) | 18,821 (18,278–18,974) |
| fifo-8conn send | 5,251 (5,244–5,273) | 5,360 (5,339–5,392) | 7,001 (6,981–7,009) |
| fifo-8conn consume | 21,769 (21,651–22,024) | 4,408 (4,153–4,868) | 6,535 (6,383–6,748) |
| 10queues-64conn send | 28,645 (27,899–30,093) | 17,844 (17,325–18,012) | 32,316 (32,102–32,353) |
| 10queues-64conn consume | 47,270 (45,464–51,589) | 10,766 (10,569–10,941) | 18,226 (18,011–18,338) |
| body-256KiB send | 593 (528–660) | 488 (453–512) | 459 (452–471) |
| body-256KiB consume | 2,133 (1,943–2,142) | 903 (867–981) | 867 (770–875) |
| body-5MiB send | 47.6 (44.3–48.5) | 29.6 (27.8–30.5) | 55.5 (53.0–57.3) |
| body-5MiB consume | 101 (100–104) | 41.0 (40.8–42.5) | 0.00 |
| 100k backlog fill, batches of 10 | 51,387 (49,026–51,411) | 20,025 (19,329–20,849) | 39,942 (38,203–40,843) |
| Consume from 100k backlog | 33,101 (32,316–33,599) | 9,723 (8,003–9,958) | 14,829 (14,816–17,097) |

### durable: latency at fixed rates

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| rate-200 achieved msg/s | 200 (200–200) | 200 (200–200) | 200 (200–200) |
| rate-200 send p50 ms | 2.57 (2.54–2.63) | 2.50 (2.48–2.51) | 2.40 (2.28–2.55) |
| rate-200 send p99 ms | 4.16 (4.01–4.84) | 4.17 (3.71–4.34) | 4.38 (4.35–5.30) |
| rate-200 send p99.9 ms | 6.54 (5.71–9.21) | 18.5 (12.3–46.1) | 10.7 (5.79–20.4) |
| rate-200 end-to-end p50 ms | 2.62 (2.60–2.68) | 2.55 (2.45–2.55) | 2.40 (2.05–2.61) |
| rate-200 end-to-end p99 ms | 4.87 (4.39–5.48) | 4.19 (3.69–4.50) | 4.33 (4.26–5.38) |
| rate-1000 achieved msg/s | 1000 (1000–1000) | 1000 (1000–1000) | 1000 (1000–1000) |
| rate-1000 send p50 ms | 1.43 (1.31–1.52) | 1.41 (1.39–1.42) | 1.44 (1.42–1.50) |
| rate-1000 send p99 ms | 2.65 (2.47–2.83) | 4.92 (2.39–5.74) | 2.64 (2.49–5.63) |
| rate-1000 send p99.9 ms | 5.53 (3.38–7.38) | 19.8 (8.31–32.7) | 9.64 (6.84–22.5) |
| rate-1000 end-to-end p50 ms | 1.57 (1.43–1.59) | 1.44 (1.41–1.46) | 1.45 (1.43–1.50) |
| rate-1000 end-to-end p99 ms | 2.95 (2.78–3.04) | 5.26 (2.56–6.06) | 2.64 (2.51–6.60) |

### durable: resources

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| Startup to first healthy response, ms | 67.8 (63.0–71.0) | 95.2 (90.6–98.6) | 1,084 (1,059–1,103) |
| Idle process memory (anon), MiB | 0.31 (0.30–0.32) | 18.5 (18.5–18.6) | 172 (168–173) |
| Idle cgroup usage (with page cache), MiB | 1.76 (1.75–1.98) | 20.7 (20.2–21.2) | 175 (170–176) |
| Peak memory, standard-64conn send, MiB | 211 (207–216) | 357 (271–487) | 1,251 (1,226–1,259) |
| Server CPU seconds per 1,000 sends, 8 conns | 0.09 (0.08–0.09) | 0.22 (0.21–0.22) | 0.16 (0.16–0.16) |
| Server CPU seconds per 1,000 receive+delete, 8 conns | 0.04 (0.04–0.04) | 0.18 (0.18–0.18) | 0.13 (0.13–0.14) |
| Server cores used, 64-conn send | 2.52 (2.47–2.59) | 3.11 (3.07–3.20) | 3.72 (3.71–3.73) |
| Load generator cores used, 64-conn send (limit 4) | 1.52 (1.50–1.53) | 0.49 (0.48–0.50) | 1.01 (0.99–1.07) |
| Memory with 1,000 long polls, MiB | 4.52 (4.44–4.53) | 201 (171–335) | 1,277 (1,256–1,284) |
| Server cores with 1,000 long polls | 0.02 (0.02–0.02) | 0.09 (0.09–0.09) | 0.14 (0.12–0.14) |
| Memory holding 100k backlog, MiB | 107 (107–107) | 301 (294–537) | 1,278 (1,259–1,284) |
| Memory growth per backlog message, bytes | 1,079 (1,079–1,080) | 1,364 (979–2,118) | 12.7 (6.80–25.0) |
| Disk with 100k backlog, MiB | 163 (148–163) | 167 (165–225) | 794 (768–813) |
| Restart with backlog: startup ms | 518 (510–587) | 513 (494–515) | 1,129 (1,118–1,136) |
| Restart with backlog: restored | 94,940 (94,930–94,940) | 94,930 | 94,930 |
| Restart with backlog: expected | 94,940 (94,930–94,940) | 94,930 | 94,930 |
| SIGKILL: acknowledged sends | 42,441 (40,941–43,317) | 39,742 (39,063–40,052) | 9,221 (7,653–9,828) |
| SIGKILL: acknowledged sends lost | 0.00 | 2,195 (1,320–2,606) | 2,581 (2,288–2,925) |
| 1 GiB limit: idle memory, MiB | – | – | – |
| 1 GiB limit: send, msg/s | – | – | – |
| 1 GiB limit: fill 50k, msg/s | – | – | – |
| 1 GiB limit: memory after 50k, MiB | – | – | – |

### Exactly-once and error checks

- elasticmq-jvm durable round 1 body-5MiB/consume: {"errors": 969, "server_died": "false 255", "first_error": "error reading a body from connection", "incomplete": "0/?"}
- elasticmq-jvm memory round 2 body-5MiB/consume: {"errors": 954, "server_died": "false 255", "first_error": "Connection refused (os error 111)", "incomplete": "0/?"}
- elasticmq-jvm durable round 2 body-5MiB/consume: {"errors": 939, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}
- elasticmq-jvm durable round 3 body-5MiB/consume: {"errors": 1839, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}

4 phases reported a problem.
0 of 410 measured phases left out for other VM work above 25%.
Other VM work during phases: median 3%, highest 23%.
Load generator: highest 2.56 of 4 cores.
