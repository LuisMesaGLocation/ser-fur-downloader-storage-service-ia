curl -X POST "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=AIzaSyDXeQOINuNrpcIO8c6aVdnuRM80QJvR8ME" \
-H "Content-Type: application/json" \
--data-raw '{
    "email": "wilson@glocation.com.co",
    "password": "wilson123",
    "returnSecureToken": true
}'
