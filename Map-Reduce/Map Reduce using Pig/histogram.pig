S = LOAD '$P' USING PigStorage(',') AS (red : long ,green : long ,blue : long);

R = GROUP S by red;
G = GROUP S by green;
--illustrate G;

B = GROUP S by blue;
--describe B;illustrate B;

RX = FOREACH R GENERATE '1', group AS x,COUNT(S.red) as r;
--describe RX; illustrate RX; dump RX;

GX = FOREACH G GENERATE '2',group AS g,COUNT(S.green) as c;
--describe GX; illustrate GX; dump GX;

BX = FOREACH B GENERATE '3',group AS b,COUNT(S.blue) as bc;
--describe GX; illustrate GX; dump GX;

X = UNION RX, GX, BX;
STORE X INTO '$O' using PigStorage (',');
--describe X;
--dump X;
