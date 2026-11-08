
### memory: throughput, messages/s, median (lowest–highest round)

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| warmup send | 19,334 (18,654–19,458) | 17,644 (17,576–18,383) | 23,497 (23,492–23,729) |
| warmup consume | 33,470 (32,405–33,908) | 38,673 (38,658–39,289) | 52,120 (48,835–52,885) |
| standard-1conn send | 6,151 (6,144–6,155) | 2,893 (2,544–2,917) | 3,721 (3,099–3,754) |
| standard-1conn consume | 18,309 (18,133–19,141) | 9,643 (8,388–9,934) | 12,100 (11,411–12,708) |
| standard-8conn send | 16,705 (15,746–16,791) | 19,134 (19,086–20,145) | 29,686 (29,198–29,779) |
| standard-8conn consume | 25,756 (25,653–25,946) | 45,473 (42,989–46,164) | 63,349 (59,855–63,543) |
| standard-64conn send | 25,765 (25,655–27,073) | 27,948 (27,629–29,353) | 53,488 (51,866–54,787) |
| standard-64conn consume | 46,301 (45,600–46,766) | 49,914 (47,877–52,707) | 74,700 (71,944–78,993) |
| fifo-8conn send | 5,560 (5,511–5,606) | 5,282 (5,270–5,463) | 7,112 (6,986–7,428) |
| fifo-8conn consume | 18,998 (18,371–19,841) | 5,049 (4,828–5,495) | 7,941 (7,161–8,296) |
| 10queues-64conn send | 23,573 (23,480–23,609) | 30,635 (29,663–31,501) | 51,984 (51,233–54,310) |
| 10queues-64conn consume | 41,872 (41,242–42,155) | 61,037 (57,036–63,004) | 84,639 (81,701–89,553) |
| body-256KiB send | 1,393 (1,290–1,465) | 608 (436–617) | 1,251 (1,237–1,376) |
| body-256KiB consume | 2,139 (1,950–2,241) | 1,092 (1,051–1,122) | 1,935 (1,762–2,109) |
| body-5MiB send | 123 (110–128) | 36.9 (33.9–41.2) | 64.5 (63.1–77.8) |
| body-5MiB consume | 98.7 (83.0–102) | 49.5 (46.8–52.0) | 0.00 (0.00–51.6) |
| 100k backlog fill, batches of 10 | 45,271 (43,081–47,030) | 53,524 (44,043–54,260) | 70,240 (66,979–71,118) |
| Consume from 100k backlog | 26,254 (25,550–26,722) | 42,257 (41,233–50,481) | 64,740 (52,170–69,986) |

### memory: latency at fixed rates

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| rate-200 achieved msg/s | 200 (200–200) | 200 (200–200) | 200 (200–200) |
| rate-200 send p50 ms | 2.70 (2.54–2.74) | 2.61 (2.48–2.79) | 2.59 (2.54–2.64) |
| rate-200 send p99 ms | 7.23 (6.68–8.51) | 5.42 (4.35–6.91) | 5.93 (5.41–5.98) |
| rate-200 send p99.9 ms | 13.0 (11.5–13.8) | 15.1 (5.67–24.6) | 11.4 (9.02–24.5) |
| rate-200 end-to-end p50 ms | 2.71 (2.52–2.76) | 2.65 (2.50–2.82) | 2.62 (2.55–2.65) |
| rate-200 end-to-end p99 ms | 7.32 (6.72–8.52) | 5.30 (4.36–6.85) | 5.96 (5.57–5.97) |
| rate-1000 achieved msg/s | 1000 (1000–1000) | 1000 (1000–1000) | 1000 (1000–1000) |
| rate-1000 send p50 ms | 1.47 (1.39–1.47) | 1.51 (1.44–1.57) | 1.42 (1.36–1.54) |
| rate-1000 send p99 ms | 2.87 (2.63–3.29) | 2.85 (2.73–3.52) | 2.73 (2.56–4.17) |
| rate-1000 send p99.9 ms | 6.64 (6.64–6.98) | 8.12 (6.65–11.4) | 6.00 (5.46–16.4) |
| rate-1000 end-to-end p50 ms | 1.42 (1.14–1.86) | 1.48 (1.45–1.60) | 1.40 (1.35–1.71) |
| rate-1000 end-to-end p99 ms | 3.29 (2.59–3.33) | 2.85 (2.74–3.56) | 3.67 (2.55–4.21) |
| rate-5000 achieved msg/s | 4,999 (4,999–4,999) | 4,999 (4,999–4,999) | 4,999 (4,999–4,999) |
| rate-5000 send p50 ms | 1.34 (1.33–1.40) | 1.20 (1.15–1.21) | 1.14 (1.10–1.21) |
| rate-5000 send p99 ms | 3.03 (2.88–3.19) | 9.57 (7.66–12.0) | 2.76 (2.68–2.83) |
| rate-5000 send p99.9 ms | 11.9 (8.19–28.5) | 36.4 (31.8–37.3) | 11.9 (11.0–12.5) |
| rate-5000 end-to-end p50 ms | 1.45 (1.45–1.80) | 1.34 (1.30–1.35) | 1.27 (1.24–1.35) |
| rate-5000 end-to-end p99 ms | 3.38 (3.24–4.39) | 11.1 (9.07–14.1) | 3.32 (3.19–3.37) |

### memory: resources

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| Startup to first healthy response, ms | 61.1 (56.3–70.2) | 97.4 (91.2–107) | 1,048 (1,031–1,050) |
| Idle process memory (anon), MiB | 0.31 (0.31–0.32) | 14.8 (14.6–14.9) | 150 (147–151) |
| Idle cgroup usage (with page cache), MiB | 1.13 (0.97–1.35) | 19.6 (19.1–20.5) | 152 (150–162) |
| Peak memory, standard-64conn send, MiB | 401 (400–403) | 343 (286–362) | 1,190 (1,136–1,207) |
| Server CPU seconds per 1,000 sends, 8 conns | 0.12 (0.11–0.12) | 0.16 (0.16–0.16) | 0.11 (0.11–0.12) |
| Server CPU seconds per 1,000 receive+delete, 8 conns | 0.07 (0.07–0.07) | 0.06 (0.06–0.06) | 0.05 (0.05–0.05) |
| Server cores used, 64-conn send | 2.51 (2.50–2.52) | 3.21 (3.18–3.23) | 3.82 (3.82–3.83) |
| Load generator cores used, 64-conn send (limit 4) | 1.20 (1.19–1.21) | 0.90 (0.86–0.91) | 1.80 (1.77–1.81) |
| Memory with 1,000 long polls, MiB | 242 (233–243) | 181 (124–997) | 1,218 (1,209–1,256) |
| Server cores with 1,000 long polls | 0.02 (0.02–0.02) | 0.09 (0.09–0.11) | 0.12 (0.12–0.13) |
| Memory holding 100k backlog, MiB | 345 (336–346) | 304 (224–1,173) | 1,220 (1,211–1,258) |
| Memory growth per backlog message, bytes | 1,079 (1,079–1,079) | 1,281 (1,050–1,837) | 25.4 (22.1–27.6) |
| Disk with 100k backlog, MiB | – | – | – |
| Restart with backlog: startup ms | – | – | – |
| Restart with backlog: restored | – | – | – |
| Restart with backlog: expected | – | – | – |
| SIGKILL: acknowledged sends | – | – | – |
| SIGKILL: acknowledged sends lost | – | – | – |
| 1 GiB limit: idle memory, MiB | 0.31 (0.31–0.32) | 14.8 (14.7–14.9) | 105 (104–106) |
| 1 GiB limit: send, msg/s | 17,935 (17,539–18,886) | 18,029 (17,802–18,044) | 19,020 (18,628–19,207) |
| 1 GiB limit: fill 50k, msg/s | 47,063 (44,444–47,854) | 59,645 (58,390–60,203) | 53,302 (52,714–55,295) |
| 1 GiB limit: memory after 50k, MiB | 204 (201–212) | 229 (146–254) | 425 (423–433) |

### durable: throughput, messages/s, median (lowest–highest round)

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| warmup send | 11,133 (10,848–11,399) | 11,292 (11,024–11,437) | 14,426 (14,234–15,090) |
| warmup consume | 28,607 (28,409–29,115) | 8,940 (8,810–9,665) | 16,162 (15,950–16,333) |
| standard-1conn send | 2,677 (2,601–2,686) | 2,424 (2,424–2,441) | 2,757 (2,734–3,008) |
| standard-1conn consume | 8,408 (7,717–8,883) | 5,271 (5,216–5,342) | 7,179 (7,071–7,808) |
| standard-8conn send | 11,021 (10,930–11,192) | 13,963 (13,929–14,204) | 21,085 (21,015–22,518) |
| standard-8conn consume | 28,767 (26,934–29,136) | 10,570 (9,954–11,070) | 17,543 (17,521–18,572) |
| standard-64conn send | 28,662 (27,947–29,595) | 16,739 (15,314–17,677) | 30,816 (28,679–32,615) |
| standard-64conn consume | 45,674 (43,833–51,447) | 10,077 (9,346–10,243) | 18,091 (17,600–18,281) |
| fifo-8conn send | 5,101 (5,085–5,137) | 5,223 (5,119–5,255) | 6,931 (6,925–7,115) |
| fifo-8conn consume | 18,133 (17,640–18,603) | 4,905 (4,304–4,953) | 6,280 (6,239–6,432) |
| 10queues-64conn send | 24,978 (24,892–25,945) | 16,956 (16,419–17,863) | 31,182 (30,878–31,469) |
| 10queues-64conn consume | 39,379 (38,780–45,260) | 10,580 (10,016–10,622) | 17,090 (17,028–17,875) |
| body-256KiB send | 548 (527–594) | 475 (457–477) | 453 (447–456) |
| body-256KiB consume | 1,834 (1,727–2,093) | 844 (820–875) | 927 (842–934) |
| body-5MiB send | 43.8 (43.8–44.3) | 27.9 (20.4–28.1) | 42.8 (40.2–53.4) |
| body-5MiB consume | 78.3 (76.4–90.9) | 37.8 (29.2–40.9) | 0.00 |
| 100k backlog fill, batches of 10 | 43,858 (42,997–45,604) | 19,211 (19,072–20,331) | 38,283 (38,261–39,692) |
| Consume from 100k backlog | 26,174 (25,656–28,333) | 8,114 (7,978–9,266) | 12,293 (12,086–17,597) |

### durable: latency at fixed rates

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| rate-200 achieved msg/s | 200 (200–200) | 200 (200–200) | 200 (200–200) |
| rate-200 send p50 ms | 2.85 (2.72–2.97) | 2.81 (2.63–2.90) | 2.78 (2.75–2.79) |
| rate-200 send p99 ms | 4.72 (4.67–4.75) | 6.02 (4.83–6.30) | 5.45 (4.67–6.40) |
| rate-200 send p99.9 ms | 8.33 (8.12–11.4) | 53.5 (13.7–58.4) | 14.6 (6.76–26.5) |
| rate-200 end-to-end p50 ms | 2.87 (2.75–2.97) | 2.85 (2.59–2.99) | 2.80 (2.76–2.80) |
| rate-200 end-to-end p99 ms | 4.92 (4.86–5.07) | 6.00 (4.78–6.59) | 5.44 (4.68–6.24) |
| rate-1000 achieved msg/s | 1000 (1000–1000) | 1000 (998–1000) | 1000 (1000–1000) |
| rate-1000 send p50 ms | 1.48 (1.45–1.55) | 1.51 (1.43–1.53) | 1.48 (1.41–1.51) |
| rate-1000 send p99 ms | 3.14 (3.11–3.23) | 2.89 (2.56–6.09) | 4.04 (3.68–4.22) |
| rate-1000 send p99.9 ms | 6.40 (6.39–6.92) | 10.5 (8.09–40.0) | 18.5 (17.7–19.4) |
| rate-1000 end-to-end p50 ms | 1.92 (1.61–2.09) | 1.52 (1.45–1.97) | 1.48 (1.48–2.58) |
| rate-1000 end-to-end p99 ms | 4.07 (3.46–4.22) | 2.93 (2.60–6.91) | 4.38 (3.92–5.39) |

### durable: resources

| Metric | rusticmq | elasticmq-native | elasticmq-jvm |
| --- | --- | --- | --- |
| Startup to first healthy response, ms | 64.6 (63.0–67.0) | 98.9 (91.2–110) | 1,079 (1,076–1,080) |
| Idle process memory (anon), MiB | 0.31 | 18.4 (18.4–18.7) | 171 (164–181) |
| Idle cgroup usage (with page cache), MiB | 1.85 (1.50–1.93) | 20.4 (19.7–20.6) | 173 (166–184) |
| Peak memory, standard-64conn send, MiB | 181 (177–187) | 337 (238–429) | 1,236 (1,167–1,256) |
| Server CPU seconds per 1,000 sends, 8 conns | 0.09 (0.09–0.09) | 0.23 (0.22–0.23) | 0.16 (0.15–0.16) |
| Server CPU seconds per 1,000 receive+delete, 8 conns | 0.05 (0.05–0.05) | 0.19 (0.18–0.20) | 0.13 (0.13–0.13) |
| Server cores used, 64-conn send | 2.37 (2.25–2.38) | 3.02 (2.91–3.18) | 3.68 (3.68–3.70) |
| Load generator cores used, 64-conn send (limit 4) | 1.38 (1.28–1.42) | 0.47 (0.44–0.48) | 0.91 (0.89–0.94) |
| Memory with 1,000 long polls, MiB | 4.21 (4.17–4.26) | 361 (207–1,056) | 1,270 (1,269–1,280) |
| Server cores with 1,000 long polls | 0.02 (0.02–0.02) | 0.10 (0.09–0.11) | 0.14 (0.13–0.15) |
| Memory holding 100k backlog, MiB | 107 (107–107) | 526 (344–1,123) | 1,272 (1,271–1,280) |
| Memory growth per backlog message, bytes | 1,079 (1,079–1,080) | 1,442 (672–1,731) | 13.1 (7.66–33.5) |
| Disk with 100k backlog, MiB | 130 (127–134) | 157 (95.4–225) | 719 (680–739) |
| Restart with backlog: startup ms | 561 (558–561) | 467 (431–499) | 1,161 (1,155–1,181) |
| Restart with backlog: restored | 94,940 (94,930–94,940) | 94,930 | 94,930 (94,930–94,940) |
| Restart with backlog: expected | 94,940 (94,930–94,940) | 94,930 | 94,930 (94,930–94,940) |
| SIGKILL: acknowledged sends | 33,173 (32,437–35,499) | 39,062 (38,520–39,370) | 8,384 (7,500–9,060) |
| SIGKILL: acknowledged sends lost | 0.00 | 2,300 (2,058–5,230) | 2,686 (2,645–2,854) |
| 1 GiB limit: idle memory, MiB | – | – | – |
| 1 GiB limit: send, msg/s | – | – | – |
| 1 GiB limit: fill 50k, msg/s | – | – | – |
| 1 GiB limit: memory after 50k, MiB | – | – | – |

### Exactly-once and error checks

- elasticmq-jvm memory round 1 body-5MiB/consume: {"errors": 1765, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}
- elasticmq-jvm durable round 1 body-5MiB/consume: {"errors": 885, "server_died": "false 255", "first_error": "Connection refused (os error 111)", "incomplete": "0/?"}
- elasticmq-jvm memory round 2 body-5MiB/consume: {"errors": 2583, "server_died": "false 255", "first_error": "Connection refused (os error 111)", "incomplete": "0/?"}
- elasticmq-jvm durable round 2 body-5MiB/consume: {"errors": 1753, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}
- elasticmq-jvm durable round 3 body-5MiB/consume: {"errors": 48, "server_died": "false 255", "first_error": "ReceiveMessage 503: The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!", "incomplete": "0/?"}

5 phases reported a problem.
0 of 409 measured phases left out for other VM work above 25%.
Other VM work during phases: median 3%, highest 21%.
Load generator: highest 2.55 of 4 cores.
