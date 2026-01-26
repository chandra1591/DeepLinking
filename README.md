## Why deep linking is useful

🔗 Open app screens from emails, SMS, push notifications

🌍 Connect web content to app content

🚀 Improve user experience (less tapping around)

📈 Better conversions (common in e-commerce, social apps)

## Android Manifest File 
```
<intent-filter>
    <action android:name="android.intent.action.VIEW" />
    <category android:name="android.intent.category.DEFAULT" />
    <category android:name="android.intent.category.BROWSABLE" />
    <data android:scheme="https"
          android:host="example.com"
          android:pathPrefix="/products" />
</intent-filter>
```
