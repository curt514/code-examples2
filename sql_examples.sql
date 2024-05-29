SELECT
    MIN(ptr1.id) refund_transaction_id, # first refund transaction id for crc32 hash
    ptr1.base_id refund_transaction_base_id,
    SUM(ptr1.sum) refund_transaction_sum, # consider more than one refund
    d.dmn dmn,
    ctt2.value inn,
    dc2.value type,
    o.order_id order_id,
    o.order_num order_num,
    o.order_contacts_email order_contacts_email,
    o.order_contacts_phone,
    ptr2.sum order_sum,
    o.external_data order_external_data
FROM prd.transaction_registry ptr1 # refund transaction
         JOIN prd.transaction_registry ptr2 ON ptr1.base_id = ptr2.id # join to main transaction, which can join to order
         JOIN i4.ds d ON ptr2.dmn_id = d.dmn_id # simply for getting dmn
         JOIN i4.ord o ON ptr2.order_id = o.order_id # join ord. Now we can get order's data
         JOIN i4.d_config dc1 ON dc1.dmn_id = ptr2.dmn_id AND dc1.name = 'ctt_number' # getting dmn config for dmn in order
         JOIN i4.d_config dc2 ON dc2.dmn_id = ptr2.dmn_id AND dc2.name = 'type' # getting type - service/goods/etc   
         JOIN prd_shared.ctt_config ctt1 ON ctt1.ctt_number = dc1.value AND ctt1.name = 'type' # join ctt_config for filter by NPD (type = self)
         JOIN prd_shared.ctt_config ctt2 ON ctt2.ctt_number = dc1.value AND ctt2.name = 'inn' # join ctt_config for getting INN
WHERE ptr1.sum IS NOT NULL # filter starnge transaction
  AND ptr1.id IN(# filter for main refund transactions ids list (function argument) 
    :refind_ids
    )
  AND ctt1.value = 'self'# filter NPD 
GROUP BY ptr1.base_id; # consider more than one refund, view on line 4 in this SQL

##################################################################################################

-- master
SELECT
    d.dmn_id,
    d.dmn,
    SUM(ptr.sum) summa,
    c3.value mode1,
    p.profile_login email1,
    p.profile_phone phone1,
    ct.value
FROM  prd.payment_transaction_registry ptr
          JOIN i4.ord o ON o.order_id = ptr.order_id
          JOIN i4.dmns d ON d.dmn_id = o.site_id
          JOIN i4.order_products op ON ptr.order_id = op.order_id
          JOIN i4.products pp ON op.product_id = pp.product_id
          JOIN i4.dmn_config c2 ON c2.dmn_id = d.dmn_id AND c2.name = 'ctt_number'
          JOIN i4.dmn_config c3 ON c3.dmn_id = d.dmn_id AND c3.name = 'mode_available'
          JOIN prd_shared.ctt_config ct ON ct.ctt_number = c2.value AND ct.name = 'name'
          JOIN prd_shared.ctt_config ctt ON ctt.ctt_number = c2.value AND ctt.name = 'type'
          JOIN prd_shared.ctt_config ctt1 ON ctt1.ctt_number = c2.value AND ctt1.name = 'profile_id'
          JOIN prd.profiles p ON p.profile_id = ctt1.value
WHERE
    ptr.type = 'payment' AND
    ctt.value = 'self' AND
    YEAR(ptr.date) = 2023 AND
    c3.value = 1
GROUP BY d.dmn_id
HAVING summa >= 2400000


