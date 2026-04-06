      *================================================================*
      * CUSTMAST.cbl                                                  *
      * Customer Master Record copybook                               *
      *                                                               *
      * Record layout for IBMUSER.VSAM.CUSTMAST KSDS cluster.        *
      * Key field: CMR-CUST-ID (offset 0, length 10)                 *
      *                                                               *
      * Total fixed record length: 143 bytes                         *
      *   CMR-CUST-ID      10                                        *
      *   CMR-LAST-NAME    25                                        *
      *   CMR-FIRST-NAME   15                                        *
      *   CMR-DOB           8                                        *
      *   CMR-STREET       30                                        *
      *   CMR-CITY         20                                        *
      *   CMR-STATE         2                                        *
      *   CMR-ZIP          10                                        *
      *   CMR-BALANCE       7  (S9(11)V99 COMP-3: 13 digits → 7)    *
      *   CMR-CREDIT-LIMIT  6  (S9(9)V99 COMP-3: 11 digits → 6)     *
      *   CMR-ACCOUNT-TYPE  1                                        *
      *   CMR-STATUS        1                                        *
      *   CMR-OPEN-DATE     8                                        *
      *   CMR-LAST-TXN-DATE 8                                        *
      *   FILLER           10                                        *
      *================================================================*
       01  CUSTOMER-MASTER-RECORD.
           05  CMR-KEY.
               10  CMR-CUST-ID       PIC X(10).
           05  CMR-PERSONAL.
               10  CMR-LAST-NAME     PIC X(25).
               10  CMR-FIRST-NAME    PIC X(15).
               10  CMR-DOB           PIC 9(8).
           05  CMR-ADDRESS.
               10  CMR-STREET        PIC X(30).
               10  CMR-CITY          PIC X(20).
               10  CMR-STATE         PIC X(2).
               10  CMR-ZIP           PIC X(10).
           05  CMR-FINANCIALS.
               10  CMR-BALANCE       PIC S9(11)V99 COMP-3.
               10  CMR-CREDIT-LIMIT  PIC S9(9)V99 COMP-3.
               10  CMR-ACCOUNT-TYPE  PIC X(1).
               10  CMR-STATUS        PIC X(1).
           05  CMR-TIMESTAMPS.
               10  CMR-OPEN-DATE     PIC 9(8).
               10  CMR-LAST-TXN-DATE PIC 9(8).
           05  FILLER                PIC X(10).
