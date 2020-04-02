M = LOAD '$M' USING PigStorage(',') AS ( i:long, j:long, V:double);
N = LOAD '$N' USING PigStorage(',') AS ( i:long, j:long, V:double);
J = JOIN M BY j FULL OUTER, N BY i;
R = FOREACH J GENERATE M::i as row, N::j as col, (M::V*N::V) as val;
T = GROUP R BY (row,col);
O = FOREACH T GENERATE group.$0 AS row, group.$1 AS col, SUM(R.val) as val;
STORE O INTO '$O' USING PigStorage (',');
