package com.codekata;

import java.io.Serializable;
import java.math.BigDecimal;

public class Balance implements Serializable {
    String account, date, ccy;
            BigDecimal amount;

    public Balance(String account, String date, String ccy, String amount) {
        this.account = account;
        this.date = date;
        this.ccy = ccy;
        this.amount = new BigDecimal(amount);
    }

    @Override
    public String toString() {
        return "Balance{" +
                "account='" + account + '\'' +
                ", date='" + date + '\'' +
                ", ccy='" + ccy + '\'' +
                ", amount='" + amount + '\'' +
                '}';
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCcy() {
        return ccy;
    }

    public void setCcy(String ccy) {
        this.ccy = ccy;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
}
