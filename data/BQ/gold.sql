/* ===================== 1) Provider Charge Summary ===================== */

CREATE TABLE IF NOT EXISTS `my-project-hospital-erm.gold_dataset.provider_charge_summary` (
  Provider_Name STRING,
  Dept_Name STRING,
  Amount FLOAT64
);

-- Truncate table
TRUNCATE TABLE `my-project-hospital-erm.gold_dataset.provider_charge_summary`;

-- Insert data
INSERT INTO `my-project-hospital-erm.gold_dataset.provider_charge_summary`
SELECT
  CONCAT(p.FirstName, ' ', p.LastName) AS Provider_Name,
  d.Name AS Dept_Name,
  SUM(t.Amount) AS Amount
FROM `my-project-hospital-erm.silver_dataset.transactions` t
LEFT JOIN `my-project-hospital-erm.silver_dataset.providers` p
  ON SPLIT(p.ProviderID, '-')[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `my-project-hospital-erm.silver_dataset.departments` d
  ON SPLIT(d.Dept_Id, '-')[SAFE_OFFSET(0)] = p.DeptID
WHERE t.is_quarantined = FALSE
  AND d.Name IS NOT NULL
GROUP BY Provider_Name, Dept_Name;


/* ===================== 2) Patient History ===================== */

CREATE TABLE IF NOT EXISTS `my-project-hospital-erm.gold_dataset.patient_history` (
  Patient_Key STRING,
  FirstName STRING,
  LastName STRING,
  Gender STRING,
  DOB INT64,
  Address STRING,
  EncounterDate INT64,
  EncounterType STRING,
  Transaction_Key STRING,
  VisitDate INT64,
  ServiceDate INT64,
  BilledAmount FLOAT64,
  PaidAmount FLOAT64,
  ClaimStatus STRING,
  ClaimAmount STRING,
  ClaimPaidAmount STRING,
  PayorType STRING
);

-- Truncate table
TRUNCATE TABLE `my-project-hospital-erm.gold_dataset.patient_history`;

-- Insert data
INSERT INTO `my-project-hospital-erm.gold_dataset.patient_history`
SELECT
  p.Patient_Key,
  p.FirstName,
  p.LastName,
  p.Gender,
  p.DOB,
  p.Address,
  e.EncounterDate,
  e.EncounterType,
  t.Transaction_Key,
  t.VisitDate,
  t.ServiceDate,
  t.Amount AS BilledAmount,
  t.PaidAmount,
  c.ClaimStatus,
  c.ClaimAmount,
  c.PaidAmount AS ClaimPaidAmount,
  c.PayorType
FROM `my-project-hospital-erm.silver_dataset.patients` p
LEFT JOIN `my-project-hospital-erm.silver_dataset.encounters` e
  ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = e.PatientID
LEFT JOIN `my-project-hospital-erm.silver_dataset.transactions` t
  ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = t.PatientID
LEFT JOIN `my-project-hospital-erm.silver_dataset.claims` c
  ON t.SRC_TransactionID = c.TransactionID
WHERE p.is_current = TRUE;


/* ===================== 3) Provider Performance ===================== */

CREATE TABLE IF NOT EXISTS `my-project-hospital-erm.gold_dataset.provider_performance` (
  ProviderID STRING,
  FirstName STRING,
  LastName STRING,
  Specialization STRING,
  TotalEncounters INT64,
  TotalTransactions INT64,
  TotalBilledAmount FLOAT64,
  TotalPaidAmount FLOAT64,
  ApprovedClaims INT64,
  TotalClaims INT64,
  ClaimApprovalRate FLOAT64
);

-- Truncate table
TRUNCATE TABLE `my-project-hospital-erm.gold_dataset.provider_performance`;

-- Insert data
INSERT INTO `my-project-hospital-erm.gold_dataset.provider_performance`
SELECT
  pr.ProviderID,
  pr.FirstName,
  pr.LastName,
  pr.Specialization,
  COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
  COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
  SUM(t.Amount) AS TotalBilledAmount,
  SUM(t.PaidAmount) AS TotalPaidAmount,
  COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
  COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
  ROUND(
    (COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END)
      / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2
  ) AS ClaimApprovalRate
FROM `my-project-hospital-erm.silver_dataset.providers` pr
LEFT JOIN `my-project-hospital-erm.silver_dataset.encounters` e
  ON SPLIT(pr.ProviderID, '-')[SAFE_OFFSET(1)] = e.ProviderID
LEFT JOIN `my-project-hospital-erm.silver_dataset.transactions` t
  ON SPLIT(pr.ProviderID, '-')[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `my-project-hospital-erm.silver_dataset.claims` c
  ON t.SRC_TransactionID = c.TransactionID
GROUP BY pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization;


/* ===================== 4) Department Performance ===================== */

CREATE TABLE IF NOT EXISTS `my-project-hospital-erm.gold_dataset.department_performance` (
  Dept_Id STRING,
  DepartmentName STRING,
  TotalEncounters INT64,
  TotalTransactions INT64,
  TotalBilledAmount FLOAT64,
  TotalPaidAmount FLOAT64,
  AvgPaymentPerTransaction FLOAT64
);

-- Truncate table
TRUNCATE TABLE `my-project-hospital-erm.gold_dataset.department_performance`;

-- Insert data
INSERT INTO `my-project-hospital-erm.gold_dataset.department_performance`
SELECT
  d.Dept_Id,
  d.Name AS DepartmentName,
  COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
  COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
  SUM(t.Amount) AS TotalBilledAmount,
  SUM(t.PaidAmount) AS TotalPaidAmount,
  AVG(t.PaidAmount) AS AvgPaymentPerTransaction
FROM `my-project-hospital-erm.silver_dataset.departments` d
LEFT JOIN `my-project-hospital-erm.silver_dataset.encounters` e
  ON SPLIT(d.Dept_Id, '-')[SAFE_OFFSET(0)] = e.DepartmentID
LEFT JOIN `my-project-hospital-erm.silver_dataset.transactions` t
  ON SPLIT(d.Dept_Id, '-')[SAFE_OFFSET(0)] = t.DeptID
WHERE d.is_quarantined = FALSE
GROUP BY d.Dept_Id, d.Name;

/* ===================== 5 & 6) Financial Metrics, Payor Performance ===================== */
/* (You noted these—if you paste the specs, I’ll generate the CREATE/TRUNCATE/INSERT for them, too.) */
/* ===================== 5) Financial Metrics (Gold) ===================== */

CREATE TABLE IF NOT EXISTS `my-project-hospital-erm.gold_dataset.financial_metrics` (
  month DATE,
  line_of_business STRING,
  total_billed_amount FLOAT64,
  total_paid_amount FLOAT64,
  outstanding_balance FLOAT64,
  total_claims INT64,
  approved_claims INT64,
  claim_approval_rate FLOAT64,
  avg_days_to_pay INT64
);

-- Truncate
TRUNCATE TABLE `my-project-hospital-erm.gold_dataset.financial_metrics`;

-- Insert
INSERT INTO `my-project-hospital-erm.gold_dataset.financial_metrics`
WITH
t_norm AS (
  SELECT
    DATE_TRUNC(SAFE.PARSE_DATE('%Y%m%d', CAST(ServiceDate AS STRING)), MONTH) AS month,
    COALESCE(LineOfBusiness, 'UNKNOWN') AS line_of_business,
    SAFE_CAST(Amount AS FLOAT64) AS amount,
    SAFE_CAST(PaidAmount AS FLOAT64) AS paid_amount,
    SAFE.PARSE_DATE('%Y%m%d', CAST(ServiceDate AS STRING)) AS svc_dt,
    SAFE.PARSE_DATE('%Y%m%d', CAST(PaidDate AS STRING)) AS paid_dt
  FROM `my-project-hospital-erm.silver_dataset.transactions`
  WHERE is_quarantined = FALSE AND is_current = TRUE
),
t_agg AS (
  SELECT
    month,
    line_of_business,
    SUM(amount) AS total_billed_amount,
    SUM(paid_amount) AS total_paid_amount,
    SUM(GREATEST(amount - IFNULL(paid_amount, 0), 0)) AS outstanding_balance,
    CAST(ROUND(AVG(DATE_DIFF(paid_dt, svc_dt, DAY))) AS INT64) AS avg_days_to_pay
  FROM t_norm
  GROUP BY month, line_of_business
),
c_agg AS (
  SELECT
    DATE_TRUNC(SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(ClaimDate AS STRING)), MONTH) AS month,
    COALESCE(LineOfBusiness, 'UNKNOWN') AS line_of_business,
    COUNT(*) AS total_claims,
    COUNTIF(ClaimStatus = 'Approved') AS approved_claims
  FROM `my-project-hospital-erm.silver_dataset.claims`
  WHERE is_quarantined = FALSE AND is_current = TRUE
  GROUP BY month, line_of_business
)
SELECT
  COALESCE(t.month, c.month) AS month,
  COALESCE(t.line_of_business, c.line_of_business) AS line_of_business,
  IFNULL(t.total_billed_amount, 0) AS total_billed_amount,
  IFNULL(t.total_paid_amount, 0) AS total_paid_amount,
  IFNULL(t.outstanding_balance, 0) AS outstanding_balance,
  IFNULL(c.total_claims, 0) AS total_claims,
  IFNULL(c.approved_claims, 0) AS approved_claims,
  CASE
    WHEN IFNULL(c.total_claims, 0) = 0 THEN 0
    ELSE ROUND( (c.approved_claims / c.total_claims) * 100, 2)
  END AS claim_approval_rate,
  IFNULL(t.avg_days_to_pay, 0) AS avg_days_to_pay
FROM t_agg t
FULL JOIN c_agg c
  ON t.month = c.month AND t.line_of_business = c.line_of_business;
  
  /* ========== 6) Payor Performance & Claims Summary (Gold) ========== */

CREATE TABLE IF NOT EXISTS `my-project-hospital-erm.gold_dataset.payor_performance_summary` (
  payor_id STRING,
  payor_type STRING,
  month DATE,
  total_claims INT64,
  approved_claims INT64,
  approval_rate FLOAT64,
  total_claim_amount FLOAT64,
  total_paid_amount FLOAT64,
  payout_ratio FLOAT64,
  avg_days_to_pay INT64
);

-- Truncate
TRUNCATE TABLE `my-project-hospital-erm.gold_dataset.payor_performance_summary`;

-- Insert
INSERT INTO `my-project-hospital-erm.gold_dataset.payor_performance_summary`
WITH
c_norm AS (
  SELECT
    COALESCE(PayorID, 'UNKNOWN') AS payor_id,
    COALESCE(PayorType, 'UNKNOWN') AS payor_type,
    DATE_TRUNC(SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(ClaimDate AS STRING)), MONTH) AS month,
    SAFE_CAST(ClaimAmount AS FLOAT64) AS claim_amount,
    SAFE_CAST(PaidAmount AS FLOAT64) AS claim_paid_amount,
    ClaimStatus,
    TransactionID
  FROM `my-project-hospital-erm.silver_dataset.claims`
  WHERE is_quarantined = FALSE AND is_current = TRUE
),
t_dates AS (
  -- bring in Service/Paid dates for avg_days_to_pay, via TransactionID linkage
  SELECT
    SRC_TransactionID AS TransactionID,
    SAFE.PARSE_DATE('%Y%m%d', CAST(ServiceDate AS STRING)) AS svc_dt,
    SAFE.PARSE_DATE('%Y%m%d', CAST(PaidDate AS STRING)) AS paid_dt
  FROM `my-project-hospital-erm.silver_dataset.transactions`
  WHERE is_quarantined = FALSE AND is_current = TRUE
),
joined AS (
  SELECT
    c.payor_id,
    c.payor_type,
    c.month,
    c.claim_amount,
    c.claim_paid_amount,
    c.ClaimStatus,
    DATE_DIFF(t.paid_dt, t.svc_dt, DAY) AS days_to_pay
  FROM c_norm c
  LEFT JOIN t_dates t
    ON c.TransactionID = t.TransactionID
),
agg AS (
  SELECT
    payor_id,
    payor_type,
    month,
    COUNT(*) AS total_claims,
    COUNTIF(ClaimStatus = 'Approved') AS approved_claims,
    SUM(IFNULL(claim_amount, 0)) AS total_claim_amount,
    SUM(IFNULL(claim_paid_amount, 0)) AS total_paid_amount,
    CAST(ROUND(AVG(days_to_pay)) AS INT64) AS avg_days_to_pay
  FROM joined
  GROUP BY payor_id, payor_type, month
)
SELECT
  payor_id,
  payor_type,
  month,
  total_claims,
  approved_claims,
  CASE WHEN total_claims = 0 THEN 0
       ELSE ROUND((approved_claims / total_claims) * 100, 2)
  END AS approval_rate,
  total_claim_amount,
  total_paid_amount,
  CASE WHEN total_claim_amount = 0 THEN 0
       ELSE ROUND(total_paid_amount / total_claim_amount, 4)
  END AS payout_ratio,
  IFNULL(avg_days_to_pay, 0) AS avg_days_to_pay
FROM agg;
