CREATE TABLE legend.employee
USING DELTA
(
`first_name` STRING NOT NULL COMMENT 'Person first name',
`last_name` STRING NOT NULL COMMENT 'Person last name',
`birth_date` DATE NOT NULL COMMENT 'Person birth date',
`gender` STRING COMMENT 'Person gender',
`id` INT NOT NULL COMMENT 'Unique identifier of a databricks employee',
`sme` STRING COMMENT 'Programming skill that person truly masters',
`joined_date` DATE NOT NULL COMMENT 'When did that person join Databricks',
`high_fives` INT COMMENT 'How many high fives did that person get'
)