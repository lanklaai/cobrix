      *================================================================*
      * TRANSHST.cbl                                                  *
      * Transaction History Record copybook                          *
      *                                                               *
      * Record layout for IBMUSER.VSAM.TRANSHST ESDS cluster.        *
      *                                                               *
      * Notable features:                                             *
      *   - Level-88 condition names (skipped by parser)             *
      *   - REDEFINES: TH-ACCOUNT-REF redefines TH-DATE-DATA         *
      *   - OCCURS 3 TIMES: TH-ITEMS array                           *
      *================================================================*
       01  TRANSACTION-HISTORY.
           05  TH-TXN-ID          PIC X(16).
           05  TH-CUST-ID         PIC X(10).
           05  TH-TXN-TYPE        PIC X(1).
               88  TH-TYPE-DEBIT      VALUE 'D'.
               88  TH-TYPE-CREDIT     VALUE 'C'.
               88  TH-TYPE-TRANSFER   VALUE 'T'.
           05  TH-AMOUNT          PIC S9(11)V99 COMP-3.
           05  TH-DESCRIPTION     PIC X(40).
           05  TH-DETAIL-DATA.
               10  TH-DATE-DATA.
                   15  TH-TXN-DATE     PIC 9(8).
                   15  TH-POST-DATE    PIC 9(8).
               10  TH-ACCOUNT-REF  REDEFINES TH-DATE-DATA PIC X(16).
           05  TH-ITEMS OCCURS 3 TIMES.
               10  TH-ITEM-SEQ     PIC 9(3).
               10  TH-ITEM-AMT     PIC S9(7)V99 COMP-3.
           05  FILLER             PIC X(5).
