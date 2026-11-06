
### memory: throughput, messages/s, median (lowest–highest round)

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| warmup send | 14,238 (13,705–14,369) | 17,954 (17,749–18,060) | 22,520 (20,871–22,762) |
| warmup consume | 18,438 (17,741–18,724) | 39,010 (38,440–40,799) | 50,850 (47,754–53,288) |
| standard-1conn send | 5,122 (4,583–5,220) | 2,907 (2,581–2,978) | 3,610 (3,223–3,772) |
| standard-1conn consume | 11,903 (11,304–12,066) | 10,158 (9,058–10,173) | 12,788 (10,776–12,863) |
| standard-8conn send | 10,315 (10,305–10,674) | 20,965 (19,718–21,012) | 29,469 (28,244–29,909) |
| standard-8conn consume | 17,010 (16,548–17,329) | 47,277 (44,775–50,592) | 63,828 (59,788–65,485) |
| standard-64conn send | 21,675 (21,082–22,048) | 29,966 (29,660–31,180) | 53,753 (51,009–55,237) |
| standard-64conn consume | 31,899 (29,349–32,109) | 50,522 (46,286–51,043) | 75,588 (71,965–78,513) |
| fifo-8conn send | 5,294 (5,279–5,452) | 5,385 (5,378–5,490) | 7,180 (7,018–7,512) |
| fifo-8conn consume | 16,327 (15,206–16,447) | 5,045 (4,895–6,222) | 8,371 (7,452–8,730) |
| 10queues-64conn send | 21,190 (20,983–21,559) | 31,115 (30,411–31,287) | 54,291 (53,999–55,370) |
| 10queues-64conn consume | 28,944 (25,823–29,051) | 59,215 (59,063–63,000) | 83,843 (77,963–89,210) |
| body-256KiB send | 1,148 (1,141–1,420) | 575 (451–682) | 1,347 (1,189–1,354) |
| body-256KiB consume | 1,052 (1,034–1,065) | 1,117 (1,006–1,240) | 1,906 (1,432–2,155) |
| body-5MiB send | 108 (107–114) | 40.1 (39.4–40.4) | 75.7 (68.5–76.1) |
| body-5MiB consume | 57.1 (53.2–57.6) | 54.2 (44.2–57.8) | 0.00 (0.00–43.2) |
| 100k backlog fill, batches of 10 | 32,821 (32,775–33,829) | 56,641 (53,055–57,393) | 68,124 (64,967–72,025) |
| Consume from 100k backlog | 16,328 (15,945–16,735) | 41,675 (11,593–45,534) | 56,477 (53,970–64,093) |

### memory: latency at fixed rates

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| rate-200 achieved msg/s | 200 (200–200) | 200 (200–200) | 200 (200–200) |
| rate-200 send p50 ms | 2.53 (2.30–2.74) | 2.65 (2.52–2.75) | 2.51 (2.24–2.58) |
| rate-200 send p99 ms | 8.49 (7.46–9.82) | 5.52 (4.34–6.10) | 5.23 (4.48–8.35) |
| rate-200 send p99.9 ms | 15.8 (13.0–37.3) | 9.68 (9.26–16.3) | 8.92 (7.57–13.4) |
| rate-200 end-to-end p50 ms | 2.57 (2.29–2.72) | 2.74 (2.52–2.74) | 2.39 (2.24–2.52) |
| rate-200 end-to-end p99 ms | 8.71 (7.46–9.87) | 5.68 (4.34–6.11) | 5.02 (4.50–8.02) |
| rate-1000 achieved msg/s | 1000 (1000–1000) | 1000 (1000–1000) | 1000 (1000–1000) |
| rate-1000 send p50 ms | 1.45 (1.39–1.52) | 1.54 (1.46–1.58) | 1.36 (1.24–1.39) |
| rate-1000 send p99 ms | 2.79 (2.61–3.19) | 4.22 (2.88–14.6) | 2.48 (2.36–2.60) |
| rate-1000 send p99.9 ms | 10.6 (5.81–16.3) | 6.76 (6.72–51.3) | 7.39 (7.12–8.93) |
| rate-1000 end-to-end p50 ms | 1.48 (1.43–1.54) | 1.55 (1.45–1.58) | 1.38 (1.35–2.20) |
| rate-1000 end-to-end p99 ms | 2.86 (2.66–3.22) | 4.32 (2.96–20.6) | 2.59 (2.46–3.34) |
| rate-5000 achieved msg/s | 4,999 (4,999–4,999) | 4,999 (4,999–4,999) | 4,999 (4,999–4,999) |
| rate-5000 send p50 ms | 1.38 (1.26–1.38) | 1.25 (1.05–1.26) | 1.18 (1.17–1.21) |
| rate-5000 send p99 ms | 3.44 (2.85–4.44) | 6.46 (5.51–7.79) | 2.64 (2.62–2.88) |
| rate-5000 send p99.9 ms | 12.2 (10.0–43.4) | 27.6 (26.7–43.2) | 17.4 (13.1–20.7) |
| rate-5000 end-to-end p50 ms | 1.50 (1.46–1.51) | 1.40 (1.19–1.40) | 1.33 (1.32–1.35) |
| rate-5000 end-to-end p99 ms | 4.81 (4.46–6.10) | 7.35 (6.31–8.98) | 3.19 (3.13–3.44) |

### memory: resources

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| Startup to first healthy response, ms | 58.2 (57.6–62.8) | 121 (95.8–257) | 1,217 (1,093–1,317) |
| Idle process memory (anon), MiB | 0.30 (0.30–0.31) | 14.8 (14.7–14.8) | 151 (151–153) |
| Idle cgroup usage (with page cache), MiB | 1.74 (1.53–2.92) | 20.5 (18.7–20.6) | 156 (153–164) |
| Peak memory, standard-64conn send, MiB | 299 (289–302) | 358 (306–364) | 1,132 (1,125–1,185) |
| Server CPU seconds per 1,000 sends, 8 conns | 0.16 (0.16–0.17) | 0.15 (0.15–0.16) | 0.11 (0.11–0.12) |
| Server CPU seconds per 1,000 receive+delete, 8 conns | 0.09 (0.09–0.10) | 0.06 (0.06–0.06) | 0.05 (0.05–0.05) |
| Server cores used, 64-conn send | 2.45 (2.43–2.49) | 3.19 (3.18–3.31) | 3.83 (3.77–3.83) |
| Load generator cores used, 64-conn send (limit 4) | 0.97 (0.95–0.98) | 0.89 (0.85–0.91) | 1.69 (1.60–1.83) |
| Memory with 1,000 long polls, MiB | 178 (172–180) | 127 (86.0–143) | 1,217 (1,213–1,228) |
| Server cores with 1,000 long polls | 0.02 (0.02–0.02) | 0.09 (0.09–0.11) | 0.12 (0.12–0.12) |
| Memory holding 100k backlog, MiB | 271 (264–272) | 230 (204–235) | 1,220 (1,212–1,229) |
| Memory growth per backlog message, bytes | 970 (970–972) | 1,137 (921–1,234) | 10.2 (-18.88–22.9) |
| Disk with 100k backlog, MiB | – | – | – |
| Restart with backlog: startup ms | – | – | – |
| Restart with backlog: restored | – | – | – |
| Restart with backlog: expected | – | – | – |
| SIGKILL: acknowledged sends | – | – | – |
| SIGKILL: acknowledged sends lost | – | – | – |
| 1 GiB limit: idle memory, MiB | 0.30 (0.30–0.30) | 14.8 (14.8–14.8) | 105 (104–109) |
| 1 GiB limit: send, msg/s | 14,685 (14,435–15,553) | 17,741 (17,004–17,937) | 18,694 (18,407–18,809) |
| 1 GiB limit: fill 50k, msg/s | 38,476 (31,590–38,549) | 62,136 (58,046–63,972) | 57,322 (55,288–58,975) |
| 1 GiB limit: memory after 50k, MiB | 171 (169–178) | 210 (129–445) | 421 (421–425) |

### durable: throughput, messages/s, median (lowest–highest round)

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| warmup send | 7,880 (7,824–8,039) | 10,927 (10,766–11,592) | 14,803 (14,360–14,926) |
| warmup consume | 13,496 (13,313–13,541) | 9,430 (7,950–9,447) | 15,843 (15,808–16,236) |
| standard-1conn send | 2,665 (2,469–2,746) | 2,386 (2,046–2,439) | 2,873 (2,726–2,895) |
| standard-1conn consume | 7,455 (7,235–7,560) | 5,331 (4,365–5,494) | 7,301 (6,572–7,525) |
| standard-8conn send | 8,538 (8,461–8,796) | 14,508 (11,508–14,599) | 20,802 (20,144–22,337) |
| standard-8conn consume | 14,181 (13,748–14,763) | 10,581 (10,273–10,796) | 16,806 (15,578–17,476) |
| standard-64conn send | 16,277 (14,308–16,674) | 17,199 (17,162–17,633) | 31,901 (30,594–32,470) |
| standard-64conn consume | 20,221 (19,302–20,467) | 10,041 (8,625–10,778) | 18,416 (18,158–18,637) |
| fifo-8conn send | 4,962 (4,945–5,003) | 5,270 (5,185–5,398) | 6,820 (6,820–6,961) |
| fifo-8conn consume | 10,987 (10,315–11,010) | 3,976 (3,307–4,687) | 6,195 (6,158–6,283) |
| 10queues-64conn send | 15,891 (15,789–16,938) | 17,274 (12,295–17,610) | 30,144 (27,977–30,662) |
| 10queues-64conn consume | 17,207 (16,433–17,898) | 10,193 (9,947–10,827) | 17,388 (17,047–17,508) |
| body-256KiB send | 479 (464–518) | 465 (452–487) | 468 (434–471) |
| body-256KiB consume | 564 (479–594) | 774 (706–821) | 856 (595–907) |
| body-5MiB send | 37.3 (35.0–42.3) | 27.2 (20.1–29.8) | 42.5 (42.1–47.2) |
| body-5MiB consume | 33.9 (33.8–36.5) | 33.5 (32.5–36.5) | 0.00 |
| 100k backlog fill, batches of 10 | 20,033 (19,204–20,868) | 16,569 (15,719–19,965) | 38,140 (31,428–41,266) |
| Consume from 100k backlog | 12,625 (11,612–13,802) | 7,387 (6,387–8,162) | 12,031 (11,458–15,930) |

### durable: latency at fixed rates

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| rate-200 achieved msg/s | 200 (200–200) | 200 (200–200) | 200 (200–200) |
| rate-200 send p50 ms | 2.95 (2.73–3.05) | 2.55 (2.52–2.87) | 2.42 (2.39–2.44) |
| rate-200 send p99 ms | 5.25 (4.15–7.71) | 5.76 (4.79–9.62) | 6.16 (3.99–6.58) |
| rate-200 send p99.9 ms | 7.86 (7.78–24.6) | 20.5 (16.6–46.0) | 20.5 (9.82–25.8) |
| rate-200 end-to-end p50 ms | 3.05 (2.72–3.15) | 2.58 (2.54–2.92) | 2.28 (1.72–2.44) |
| rate-200 end-to-end p99 ms | 5.62 (4.29–8.45) | 5.75 (4.79–9.47) | 6.14 (3.71–6.72) |
| rate-1000 achieved msg/s | 1000 (1000–1000) | 1000 (999–1000) | 1000 (1000–1000) |
| rate-1000 send p50 ms | 1.42 (1.30–1.49) | 1.46 (1.24–1.48) | 1.47 (1.26–1.50) |
| rate-1000 send p99 ms | 3.03 (2.96–3.11) | 3.21 (2.79–118) | 2.61 (2.46–3.94) |
| rate-1000 send p99.9 ms | 6.30 (3.61–6.79) | 16.7 (9.43–203) | 11.6 (7.37–24.3) |
| rate-1000 end-to-end p50 ms | 1.54 (1.47–5.77) | 1.47 (1.26–1.51) | 1.47 (1.27–1.52) |
| rate-1000 end-to-end p99 ms | 3.32 (3.15–7.69) | 3.69 (2.93–144) | 2.65 (2.47–4.23) |

### durable: resources

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| Startup to first healthy response, ms | 67.9 (67.7–71.0) | 100.0 (92.2–157) | 1,149 (1,103–1,239) |
| Idle process memory (anon), MiB | 0.30 | 18.4 (18.4–18.6) | 174 (162–175) |
| Idle cgroup usage (with page cache), MiB | 1.75 (1.32–1.84) | 20.5 (19.9–20.7) | 178 (164–188) |
| Peak memory, standard-64conn send, MiB | 94.1 (83.1–96.4) | 351 (267–379) | 1,259 (1,100–1,267) |
| Server CPU seconds per 1,000 sends, 8 conns | 0.09 (0.09–0.09) | 0.22 (0.22–0.26) | 0.16 (0.15–0.17) |
| Server CPU seconds per 1,000 receive+delete, 8 conns | 0.05 (0.05–0.05) | 0.19 (0.18–0.19) | 0.14 (0.14–0.15) |
| Server cores used, 64-conn send | 1.23 (1.07–1.30) | 3.11 (3.06–3.17) | 3.70 (3.69–3.71) |
| Load generator cores used, 64-conn send (limit 4) | 0.77 (0.68–0.79) | 0.47 (0.45–0.50) | 0.95 (0.92–1.01) |
| Memory with 1,000 long polls, MiB | 3.69 (3.63–3.69) | 435 (162–617) | 1,282 (1,238–1,289) |
| Server cores with 1,000 long polls | 0.02 (0.01–0.02) | 0.11 (0.07–0.11) | 0.13 (0.10–0.14) |
| Memory holding 100k backlog, MiB | 96.3 (96.3–96.3) | 626 (288–824) | 1,283 (1,240–1,290) |
| Memory growth per backlog message, bytes | 971 (971–972) | 1,999 (1,322–2,172) | 14.0 (13.3–16.1) |
| Disk with 100k backlog, MiB | 89.9 (89.4–91.4) | 205 (189–262) | 738 (667–753) |
| Restart with backlog: startup ms | 826 (686–1,026) | 519 (473–522) | 1,205 (1,198–1,221) |
| Restart with backlog: restored | 94,930 | 94,930 | 94,930 |
| Restart with backlog: expected | 94,930 | 94,930 | 94,930 |
| SIGKILL: acknowledged sends | 24,626 (24,495–27,033) | 37,728 (28,365–39,658) | 7,858 (7,811–9,079) |
| SIGKILL: acknowledged sends lost | 0.00 | 2,814 (1,942–3,995) | 2,662 (2,262–3,143) |
| 1 GiB limit: idle memory, MiB | – | – | – |
| 1 GiB limit: send, msg/s | – | – | – |
| 1 GiB limit: fill 50k, msg/s | – | – | – |
| 1 GiB limit: memory after 50k, MiB | – | – | – |

### Exactly-once and error checks

- elasticmq-jvm memory round 1 body-5MiB/consume: {"errors": 2681, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}
- elasticmq-jvm durable round 1 body-5MiB/consume: {"errors": 946, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}
- elasticmq-jvm durable round 2 body-5MiB/consume: {"errors": 942, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}
- elasticmq-jvm memory round 3 body-5MiB/consume: {"errors": 1843, "server_died": "false 255", "first_error": "Connection refused (os error 111)", "incomplete": "0/?"}
- elasticmq-jvm durable round 3 body-5MiB/consume: {"errors": 947, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}

5 phases reported a problem.
0 of 409 measured phases left out for other VM work above 25%.
Other VM work during phases: median 3%, highest 22%.
Load generator: highest 2.60 of 4 cores.
