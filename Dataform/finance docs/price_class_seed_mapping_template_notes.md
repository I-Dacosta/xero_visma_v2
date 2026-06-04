**Purpose**
Use [price_class_seed_mapping_template.csv](</Volumes/Lagring/Aquatiq/Aquatiq integrasjonen /apps/visma_service/Dataform/finance docs/price_class_seed_mapping_template.csv>) to validate every currently observed candidate code before we promote customer price class into the model.

**How To Read It**
- One row = one observed code per `tenant_id + organization_name + source_field + source_code`.
- `source_field = customerClassId` means the code comes from `visma_customers` and is a customer-membership signal.
- `source_field = customer_sales_prices.CustomerPriceClass` means the code comes from item price lists and is a price-code signal.
- `distinct_customer_count` and `sample_customers` help identify what a `customerClassId` appears to represent.
- `distinct_item_count` and `sample_items` help identify what a `CustomerPriceClass` code appears to represent.
- `candidate_lookup_*` shows any matching lookup signal from `visma_attributes.SALESRESTR`.
  This is currently partial, but it already confirms examples like `10 = Mowi`.

**Columns To Fill**
- `validated_price_class_id`
  Put the final canonical price class id here if the row belongs to a real price class.
- `validated_price_class_name`
  Put the business name here, for example `Mowi`, `Nortura`, or another validated class name.
- `validated_customer_group_name`
  Use this if the row is actually a customer group/class and not a price class.
- `include_in_final_mapping`
  Use `yes` or `no`.
- `mapping_status`
  Suggested values: `approved`, `rejected`, `needs_source_fix`, `needs_manual_mapping`.
- `notes`
  Explain edge cases, tenant-specific exceptions, or why a row should not be used.

**Decision Rule**
- If business validates a row as a real customer price class, we can seed it into a mapping table.
- If business says the row is only a customer class/group, we keep it out of `price_class`.
- If the signal is ambiguous, we should not model it until source or mapping is clarified.
