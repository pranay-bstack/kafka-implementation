┌──────────────────────────────────────────────────────────┐
│                     MAIN THREAD                          │
│                  (JavaScript - Single threaded)          │
│                                                          │
│  ┌──────────────┐                                        │
│  │   Socket 1   │──┐                                     │
│  └──────────────┘  │                                     │
│                    │  data arrives                       │
│  ┌──────────────┐  │                                     │
│  │   Socket 2   │──┼──→ handleConnection()               │
│  └──────────────┘  │                                     │
│                    │                                     │
│  ┌──────────────┐  │                                     │
│  │   Socket 3   │──┘                                     │
│  └──────────────┘                                        │
│                                                          │
│         ↓                                                │
│  ┌─────────────────────────────────┐                     │
│  │   requestQueue = []             │                     │
│  │   [req1, req2, req3, ...]       │  ← Just tracking    │
│  └─────────────────────────────────┘                     │
│         ↓                                                │
│  worker.postMessage(request) ────────────────────┐       │
│         ↓                                        │       │
└─────────┼────────────────────────────────────────┼───────┘
          │                                        │
          │  Serializes data                       │
          │  (converts to bytes)                   │
          │                                        │
          ▼                                        │
    ┌──────────────────────────────────┐           │
    │   Internal Message Channel        │          │
    │   (Node.js handles this)          │          │
    │                                   │          │
    │   [Buffer of serialized data]    │           │
    └──────────────────────────────────┘           │
          │                                        │
          │  Deserializes data                     │
          │  (back to objects)                     │
          │                                        │
          ▼                                        │
┌──────────────────────────────────────────────────┼───────┐
│                  WORKER THREAD 1                 │       │
│              (Separate JavaScript instance)      │       │
│                                                  │       │
│  parentPort.on('message', (request) => {         │       │
│      // Process request                          │       │
│      const result = doWork(request);             │       │
│                                                  │       │
│      parentPort.postMessage(result); ────────────┼────┐  │
│  });                                             │    │  │
│                                                  │    │  │
└──────────────────────────────────────────────────┘    │  │
          ▲                                             │  │
          │                                             │  │
          │  Message Channel (reverse direction)        │  │
          └─────────────────────────────────────────────┘  │
                                                           │
┌──────────────────────────────────────────────────────────┘
│                  WORKER THREAD 2                 
│              (Separate JavaScript instance)      
│                                                  
│  parentPort.on('message', (request) => {        
│      // Process request                         
│  });                                             
│                                                  
└──────────────────────────────────────────────────