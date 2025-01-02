import pickle 

categories = ['grocery_pos', 'gas_transport', 'shopping_net', 'misc_net', 'shopping_pos',
              'travel', 'grocery_net', 'misc_pos', 'health_fitness', 'kids_pets',
              'entertainment', 'food_dining', 'home', 'personal_care']

encoder = pickle.load(open('label_encoder.pkl', 'rb'))
scaler = pickle.load(open('scaler.pkl', 'rb'))

print("Loaded classes:", encoder.classes_)

# Test encoding
test_category = 'shopping_pos'
encoded_value = encoder.transform([test_category])[0]
print(f"Encoded value for '{test_category}':", encoded_value)
