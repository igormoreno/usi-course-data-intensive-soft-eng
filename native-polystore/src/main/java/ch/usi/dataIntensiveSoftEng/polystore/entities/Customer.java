package ch.usi.dataIntensiveSoftEng.polystore.entities;

import java.util.Objects;

public class Customer {

    private final String id;
    private final String address;
    private final String city;
    private final String companyName;
    private final String contactName;
    private final String contactTitle;
    private final String country;
    private final String fax;
    private final String phone;
    private final String postalCode;
    private final String region;

    public Customer(String id, String address, String city, String companyName, String contactName, String contactTitle, String country, String fax, String phone, String postalCode, String region) {
        this.id = id;
        this.address = address;
        this.city = city;
        this.companyName = companyName;
        this.contactName = contactName;
        this.contactTitle = contactTitle;
        this.country = country;
        this.fax = fax;
        this.phone = phone;
        this.postalCode = postalCode;
        this.region = region;
    }

    public String getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public String getCity() {
        return city;
    }

    public String getCompanyName() {
        return companyName;
    }

    public String getContactName() {
        return contactName;
    }

    public String getContactTitle() {
        return contactTitle;
    }

    public String getCountry() {
        return country;
    }

    public String getFax() {
        return fax;
    }

    public String getPhone() {
        return phone;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public String getRegion() {
        return region;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Customer customer = (Customer) o;
        return Objects.equals(id, customer.id) &&
                Objects.equals(address, customer.address) &&
                Objects.equals(city, customer.city) &&
                Objects.equals(companyName, customer.companyName) &&
                Objects.equals(contactName, customer.contactName) &&
                Objects.equals(contactTitle, customer.contactTitle) &&
                Objects.equals(country, customer.country) &&
                Objects.equals(fax, customer.fax) &&
                Objects.equals(phone, customer.phone) &&
                Objects.equals(postalCode, customer.postalCode) &&
                Objects.equals(region, customer.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, address, city, companyName, contactName, contactTitle, country, fax, phone, postalCode, region);
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id='" + id + '\'' +
                ", address='" + address + '\'' +
                ", city='" + city + '\'' +
                ", companyName='" + companyName + '\'' +
                ", contactName='" + contactName + '\'' +
                ", contactTitle='" + contactTitle + '\'' +
                ", country='" + country + '\'' +
                ", fax='" + fax + '\'' +
                ", phone='" + phone + '\'' +
                ", postalCode='" + postalCode + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
