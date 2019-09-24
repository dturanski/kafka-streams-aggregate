package kafka.streams.inventory.count;

import java.util.Objects;

public class ProductKey {
    private String productCode;

    public ProductKey() {
    }


    public ProductKey(String productCode) {
        this.productCode = productCode;
    }

    public String getProductCode() {
        return productCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductKey key = (ProductKey) o;
        return Objects.equals(productCode, key.productCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productCode);
    }
}
