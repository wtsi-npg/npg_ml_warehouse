# Dataset Description

See ml warehouse in t/data/fixtures/wh_pacbio2

## TRACTION-RUN-333

  mysql> select well_label, id_pac_bio_product from pac_bio_run_well_metrics where pac_bio_run_name = 'TRACTION-RUN-333';
  +------------+------------------------------------------------------------------+
  | well_label | id_pac_bio_product                                               |
  +------------+------------------------------------------------------------------+
  | A1         | 5a0d0383da70b962afca0e75761fa494f902efc0de5b1df270dc2e3f5d24baad |
  | B1         | aecfaaa6607fb8fd4bfc2fcf36ae77058a53cbf105e7279eaba13f4ee1ac96b7 |
  | C1         | 77a0d08c510839ff4a4aaf25ce49ae089651eb333a0f19e5d0115ada85e800c8 |
  | D1         | ad26d84096fc2a3b9bbfaedf0a0b774516312a2c7b519a3d1465c2798120d0c4 |
  | E1         | ee77dc1824d8b1b135bd418d6c43a26bd1c67b8be37003ede309bf92a28972e7 |
  | F1         | 353bff3320ae1d68aa1d6491a24994b000dc8bb5febf8ec3a80206b66fc57ebe |
  +------------+------------------------------------------------------------------+

  mysql> select well_label, count(*) from pac_bio_run where pac_bio_run_name = 'TRACTION-RUN-333' group by well_label;
  +------------+----------+
  | well_label | count(*) |
  +------------+----------+
  | A1         |        1 |
  | B1         |        1 |
  | C1         |        1 |
  | D1         |        1 |
  | E1         |        1 |
  | F1         |        1 |
  +------------+----------+

## TRACTION-RUN-351

  mysql> select well_label, id_pac_bio_product from pac_bio_run_well_metrics where pac_bio_run_name = 'TRACTION-RUN-351';
  +------------+------------------------------------------------------------------+
  | well_label | id_pac_bio_product                                               |
  +------------+------------------------------------------------------------------+
  | A1         | 27793952ce49adcb1571d942ad961193e9d6fffbfed57c138be01340a186e7e6 |
  | B1         | 8227ad8b75dbcd0cf0c7db35fd35d8eb95c7a6d500b56e4fcc43fe5cd12dc587 |
  | C1         | 4c8631d2bd634bcc6ab8ebcf60f1c26b2fd32fc1c9ee5ac3dec43d76e47ca2d6 |
  | D1         | b0f4d9c7b1d9db86965655cc58052f8943152324f72143dd7e81f1590b911b49 |
  +------------+------------------------------------------------------------------+

  mysql> select well_label, count(*) from pac_bio_run where pac_bio_run_name = 'TRACTION-RUN-351' group by well_label;
  +------------+----------+
  | well_label | count(*) |
  +------------+----------+
  | A1         |        2 |
  | B1         |        1 |
  | C1         |        2 |
  | D1         |        2 |
  +------------+----------+

## TRACTION-RUN-92

  mysql> select well_label, count(*) from pac_bio_run where pac_bio_run_name = 'TRACTION-RUN-92' group by well_label;
  +------------+----------+
  | well_label | count(*) |
  +------------+----------+
  | A1         |        1 |
  | B1         |        1 |
  | C1         |        1 |
  | D1         |       40 |
  +------------+----------+

  mysql> select well_label, id_pac_bio_product from pac_bio_run_well_metrics where pac_bio_run_name = 'TRACTION-RUN-92';
  +------------+------------------------------------------------------------------+
  | well_label | id_pac_bio_product                                               |
  +------------+------------------------------------------------------------------+
  | A1         | cf18bd66e0f0895ea728c1d08103c62d3de8a57a5f879cee45f7b0acc028aa61 |
  | B1         | 63fb9a37ff19c248fc7d99bd254a61085226ded540de7c5445daf1398e339833 |
  | C1         | a65eae06f3048a186aeb9104d0a8d3f46ca59dff7747eec9918fcfa85587a3c2 |
  | D1         | c5babd5516f7b9faab8415927e5f300d5152bb96b8b922e768d876469a14fa5d |
  +------------+------------------------------------------------------------------+
