# Notices:
- befoe using filters make sure to update database first (load_quotes())

# Presumptions about tsetmc inner workings:
- Identity of an instrument doesn't change over time (i.e cSecVal doesn't change one day). if it does get_identity() logic needs to change.