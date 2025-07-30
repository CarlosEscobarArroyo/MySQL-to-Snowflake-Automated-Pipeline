CREATE OR REPLACE TABLE FACT_PEDIDOS AS
SELECT
    d."ccodpedido" AS id_pedido,
    d."ccodcampana" AS id_campana,
    d."ccodcliente" AS id_vendedor,
    f."Id" AS id_fecha,
    v."id_coordinadora",
    u."id_ubicacion",
    d."nmonpedido" AS monto_total_pedido,
    d."nmonpago" AS monto_pagado,
    IFF(
        TO_CHAR(v."fecha_ingreso", 'YYYY-MM') = TO_CHAR(d."dfecpedido", 'YYYY-MM'),
        1, 0
    ) AS es_nueva_vendedora,
    d."Tipo"
FROM "STG_DOCUMENTO" d
LEFT JOIN "DIM_FECHA" f ON d."dfecpedido" = f."Date"
LEFT JOIN "DIM_VENDEDOR" v ON d."ccodcliente" = v."id_vendedor"
LEFT JOIN "DIM_UBICACION" u ON v."ccodubigeo" = u."ccodubigeo";
