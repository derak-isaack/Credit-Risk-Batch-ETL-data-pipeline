CREATE TABLE possible_defaulters (
    ID INT PRIMARY KEY,
    Salary INT,
    Age INT,
    Experience INT,
    MaritalStatus VARCHAR(10),
    HousingStatus VARCHAR(10),
    HasCar VARCHAR(3),
    Occupation VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(50),
    yrs_employed INT,
    house_age INT,
    risk_level INT
);

CREATE TABLE non_defaulters (
    ID INT PRIMARY KEY,
    Salary INT,
    Age INT,
    Experience INT,
    MaritalStatus VARCHAR(10),
    HousingStatus VARCHAR(10),
    HasCar VARCHAR(3),
    Occupation VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(50),
    yrs_employed INT,
    house_age INT,
    risk_level INT
);