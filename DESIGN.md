Broker registry
    ↓
LocalBroker
    ↓
LocalConnection
    ├── CircularBuffer A-to-B
    ├── CircularBuffer B-to-A
    └── connection state
         ↓
LocalChannel endpoint A
LocalChannel endpoint B