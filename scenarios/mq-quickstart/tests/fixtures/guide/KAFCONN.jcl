//KAFCONN PROC JUSER='KAFUSER',                      <-- USERID FOR THE STC
//             KAFHOME='/u/kafconn/confluent',             <-- KAFKA INSTALL DIR
//             KAFVER='confluent-connect-7.5.2-zOS',    <-- VERSION SUBDIR
//             CNFPROP='connect-distributed.properties', <-- CONFIG FILE
//             MQHOME='/usr/lpp/mqm/V9R2M0'             <-- IBM MQ INSTALL PATH
//*-------------------------------------------------------------------
//* KAFKA CONNECT DISTRIBUTED STARTED TASK FOR z/OS
//*
//* THIS PROC STARTS THE KAFKA CONNECT DISTRIBUTED WORKER
//* USING BPXBATCH TO RUN THE SHELL SCRIPT FROM USS.
//*
//* BASED ON CONFLUENT CONNECT ON Z/OS QUICKSTART GUIDE
//*-------------------------------------------------------------------
//CONNECT EXEC PGM=BPXBATCH,REGION=0M,TIME=NOLIMIT,
//        PARM='SH &KAFHOME./&KAFVER./bin/connect-distributed &KAFHOME./&KAFVER./etc/kafka/&CNFPROP.'
//*
//*-------------------------------------------------------------------
//* ENVIRONMENT SETUP - CRITICAL FOR MQ INTEGRATION
//*-------------------------------------------------------------------
//STDENV DD PATH='&KAFHOME./config/connect.env',PATHOPTS=ORDONLY
//*
//*-------------------------------------------------------------------
//* STEPLIB FOR IBM MQ NATIVE LIBRARIES (SCSQAUTH & SCSQANLE)
//* REQUIRED FOR BINDINGS MODE CONNECTION TO MQ
//*-------------------------------------------------------------------
//STEPLIB DD DSN=CSQ.SCSQAUTH,DISP=SHR
//         DD DSN=CSQ.SCSQANLE,DISP=SHR
//*
//*-------------------------------------------------------------------
//* STANDARD I/O - CAPTURE OUTPUT TO JES SPOOL
//*-------------------------------------------------------------------
//STDOUT DD SYSOUT=*
//STDERR DD SYSOUT=*
//*
//*-------------------------------------------------------------------
//* STARTUP COMMAND: S KAFCONN
//* SHUTDOWN COMMAND: P KAFCONN (GRACEFUL) or F KAFCONN,SHUTDOWN
//* CANCEL COMMAND: C KAFCONN (FORCE STOP)
//*-------------------------------------------------------------------
//         PEND
