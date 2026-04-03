from faker import Faker
from faker_food import FoodProvider

# Initialize Faker and add the FoodProvider
fake = Faker()
fake.add_provider(FoodProvider)

# Generate restaurant data
print("Restaurant Name:", fake.restaurant_name())
print("Food Category:", fake.food_category())
print("Dish:", fake.dish())
print("Address:", fake.address())
