CREATE OR REPLACE TABLE FACT_PEDIDOS_DETALLE AS
SELECT
    d."ccodpedido" AS id_pedido,
    p."ID_CAMPANA" AS id_campana,
    p."ID_VENDEDOR" AS id_vendedor,
    p."ID_FECHA" AS id_fecha,
    p."id_ubicacion" AS id_ubicacion,
    d."idpedido" AS id_pedido_detalle,
    d."ccodcontenido" AS id_producto,
    TRY_CAST(d."ncantidad" AS FLOAT) AS cantidad,
    TRY_CAST(d."nimporte" AS FLOAT) AS importe_producto
FROM STG_DOCUMENTODETALLE AS d
LEFT JOIN FACT_PEDIDOS p ON d."ccodpedido" = p."ID_PEDIDO"
LEFT JOIN DIM_PRODUCTO pr ON d."ccodcontenido" = pr."id_producto";
DROP TABLE IF EXISTS STG_DOCUMENTO;
DROP TABLE IF EXISTS STG_DOCUMENTODETALLE;