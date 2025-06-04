-- 📌 Configuración inicial
USE DATABASE practicas_nubita;
USE SCHEMA public;
USE WAREHOUSE COMPUTE_WH;

-- 📊 Clasificación ABC
CREATE OR REPLACE TEMP TABLE abc_base AS
SELECT
    InventoryId,
    Brand,
    Description,
    SUM(SalesDollars) AS total_sales
FROM sales
GROUP BY InventoryId, Brand, Description;

CREATE OR REPLACE TEMP TABLE abc_ranked AS
SELECT *,
       total_sales * 100.0 / SUM(total_sales) OVER () AS pct_of_total,
       SUM(total_sales) OVER (ORDER BY total_sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
           * 100.0 / SUM(total_sales) OVER () AS cumulative_pct
FROM abc_base
ORDER BY total_sales DESC;

SELECT abc_class, COUNT(*) AS num_products
FROM (
    SELECT *,
           CASE
               WHEN cumulative_pct <= 80 THEN 'A'
               WHEN cumulative_pct <= 95 THEN 'B'
               ELSE 'C'
           END AS abc_class
    FROM abc_ranked
)
GROUP BY abc_class
ORDER BY abc_class;

-- 🔄 Rotación de inventario
CREATE OR REPLACE TEMP TABLE inv_avg AS
SELECT
    i.InventoryId,
    COALESCE(ib.onHand, 0) AS beg_stock,
    COALESCE(ie.onHand, 0) AS end_stock,
    (COALESCE(ib.onHand, 0) + COALESCE(ie.onHand, 0)) / 2.0 AS avg_stock
FROM
    (SELECT DISTINCT InventoryId FROM sales) i
LEFT JOIN inventory_beginning ib ON i.InventoryId = ib.InventoryId
LEFT JOIN inventory_end ie ON i.InventoryId = ie.InventoryId;

CREATE OR REPLACE TEMP TABLE sales_total AS
SELECT
    InventoryId,
    SUM(SalesQuantity) AS total_units
FROM sales
GROUP BY InventoryId;

SELECT
    inv_avg.InventoryId,
    inv_avg.avg_stock,
    s.total_units,
    s.total_units / NULLIF(inv_avg.avg_stock, 0) AS turnover_ratio
FROM inv_avg
LEFT JOIN sales_total s ON inv_avg.InventoryId = s.InventoryId
WHERE inv_avg.avg_stock > 0
ORDER BY turnover_ratio DESC;

-- Creamos tabla final con índice de rotación
CREATE OR REPLACE TABLE INVENTORY_TURNOVER_CLEAN AS
SELECT
  d.InventoryId,
  d.Brand,
  d.Description,
  d.Size,
  d.Ventas_Anuales,
  i.Inventario_Promedio,
  CASE 
    WHEN i.Inventario_Promedio > 0 THEN ROUND(d.Ventas_Anuales / i.Inventario_Promedio, 2)
    ELSE NULL
  END AS Rotacion_Inventario
FROM DEMANDA_ANUAL_ROTACION d
JOIN INVENTARIO_PROMEDIO_ROTACION i
  ON d.InventoryId = i.InventoryId
  AND d.Brand = i.Brand
  AND d.Description = i.Description
  AND d.Size = i.Size;

-- Creamos tabla con productos de menor rotación
CREATE OR REPLACE TABLE MENOR_ROTACION AS
SELECT *
FROM INVENTORY_TURNOVER_CLEAN
WHERE Rotacion_Inventario IS NOT NULL
ORDER BY Rotacion_Inventario ASC
LIMIT 10;

-- Creamos tabla con productos de mayor rotación
CREATE OR REPLACE TABLE MAYOR_ROTACION AS
SELECT *
FROM INVENTORY_TURNOVER_CLEAN
WHERE Rotacion_Inventario IS NOT NULL
ORDER BY Rotacion_Inventario DESC
LIMIT 10;

-- 💰 Coste de almacenamiento
CREATE OR REPLACE TEMP TABLE cost_unit AS
SELECT
  UPPER(TRIM(Brand)) AS Brand,
  UPPER(TRIM(Description)) AS Description,
  TRIM(Size) AS Size,
  AVG(PurchasePrice) AS avg_unit_cost
FROM purchase_prices
WHERE PurchasePrice IS NOT NULL
GROUP BY Brand, Description, Size;

CREATE OR REPLACE TABLE storage_cost_summary AS
SELECT
  p.Brand,
  p.Description,
  p.Size,
  SUM(ia.avg_stock) AS avg_stock,
  AVG(cu.avg_unit_cost) AS avg_unit_cost,
  SUM(ia.avg_stock * cu.avg_unit_cost * 0.25) AS storage_cost_estimate
FROM inv_avg ia
JOIN (
    SELECT DISTINCT InventoryId, Brand, Description, Size
    FROM sales
) p ON ia.InventoryId = p.InventoryId
JOIN cost_unit cu ON
  p.Brand = cu.Brand AND
  p.Description = cu.Description AND
  p.Size = cu.Size
GROUP BY p.Brand, p.Description, p.Size
ORDER BY storage_cost_estimate DESC;

-- 📦 EOQ (Cantidad Económica de Pedido)
CREATE OR REPLACE TEMP TABLE DemandaAnual AS
SELECT
  InventoryId,
  SUM(SalesQuantity) AS Demanda_Anual
FROM sales
GROUP BY InventoryId;

CREATE OR REPLACE TEMP TABLE CosteUnitarioMedio AS
SELECT
  InventoryId,
  AVG(PurchasePrice) AS Coste_Unitario_Medio
FROM PURCHASE_PRICES_EOQ_EXTENDED
WHERE PurchasePrice IS NOT NULL
GROUP BY InventoryId;

CREATE OR REPLACE TABLE EOQ_Result AS
SELECT
  d.InventoryId,
  s.Brand,
  s.Description,
  s.Size,
  d.Demanda_Anual,
  c.Coste_Unitario_Medio,
  0.25 * c.Coste_Unitario_Medio AS H,
  100 AS S,
  SQRT(2 * d.Demanda_Anual * 100 / (0.25 * c.Coste_Unitario_Medio)) AS EOQ
FROM DemandaAnual d
JOIN CosteUnitarioMedio c ON d.InventoryId = c.InventoryId
JOIN (
    SELECT DISTINCT InventoryId, Brand, Description, Size
    FROM sales
) s ON d.InventoryId = s.InventoryId
WHERE d.Demanda_Anual IS NOT NULL
  AND c.Coste_Unitario_Medio IS NOT NULL
  AND c.Coste_Unitario_Medio > 0
  AND d.Demanda_Anual > 0;

-- 📉 Punto de reorden (ROP)
CREATE OR REPLACE TABLE ROP_Result_Agrupado AS
SELECT
  Brand,
  Description,
  Size,
  SUM(Demanda_Anual) AS Total_Demanda_Anual,
  AVG(Coste_Unitario_Medio) AS Avg_Coste_Unitario_Medio,
  15 AS LeadTime_Days,
  ROUND(SUM(Demanda_Anual) / 365.0 * 15, 2) AS ROP
FROM EOQ_Result
WHERE Demanda_Anual > 0 AND Coste_Unitario_Medio > 0
GROUP BY Brand, Description, Size;

-- 🚚 Tiempo medio de entrega
CREATE OR REPLACE TABLE INVOICE_PURCHASES (
  VendorNumber VARCHAR,
  VendorName VARCHAR,
  InvoiceDate DATE,
  PONumber VARCHAR,
  PODate DATE,
  PayDate DATE,
  Quantity FLOAT,
  Dollars FLOAT,
  Freight FLOAT,
  Approval VARCHAR
);

CREATE OR REPLACE TEMP TABLE LeadTimeProveedores AS
SELECT
  VendorName,
  AVG(DATEDIFF(DAY, PODate, InvoiceDate)) AS Media_LeadTime
FROM INVOICE_PURCHASES
WHERE PODate IS NOT NULL AND InvoiceDate IS NOT NULL
GROUP BY VendorName;

CREATE OR REPLACE TABLE Productos_Con_LeadTime AS
SELECT
  s.InventoryId,
  s.Brand,
  s.Description,
  s.Size,
  s.VendorName,
  l.Media_LeadTime
FROM (
    SELECT DISTINCT InventoryId, Brand, Description, Size, VendorName
    FROM sales
) s
LEFT JOIN LeadTimeProveedores l ON s.VendorName = l.VendorName
WHERE l.Media_LeadTime IS NOT NULL;

CREATE OR REPLACE TABLE Productos_LeadTime_Agrupado AS
SELECT
  Brand,
  Description,
  Size,
  AVG(Media_LeadTime) AS Avg_LeadTime
FROM Productos_Con_LeadTime
GROUP BY Brand, Description, Size;
