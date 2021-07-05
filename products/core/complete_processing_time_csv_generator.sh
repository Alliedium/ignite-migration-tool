
maxRecords=1000000
recordsStep=50000
Xmx=200m

# ram(in-memory) mode, uses default Xmx from Dockerfile-ignite
./generate_performance_report.sh resultsFileName=ram-mode-report maxRecords=$maxRecords recordsStep=$recordsStep \
                    ignitePersistence=false

# persistenceEnabled: true, walMode: none, Xmx200m,
./generate_performance_report.sh resultsFileName=none-mode-report maxRecords=$maxRecords recordsStep=$recordsStep \
                    igniteWalMode=NONE igniteXmx=$Xmx

# persistenceEnabled: true, walMode: fsync, Xmx200m,
./generate_performance_report.sh resultsFileName=fsync-mode-report maxRecords=$maxRecords recordsStep=$recordsStep \
                    igniteWalMode=FSYNC igniteXmx=$Xmx

# persistenceEnabled: true, walMode: log-only, Xmx200m,
./generate_performance_report.sh resultsFileName=log-only-mode-report maxRecords=$maxRecords recordsStep=$recordsStep \
                    igniteWalMode=LOG_ONLY igniteXmx=$Xmx

# persistenceEnabled: true, walMode: background, Xmx200m,
./generate_performance_report.sh resultsFileName=background-mode-report maxRecords=$maxRecords recordsStep=$recordsStep \
                    igniteWalMode=BACKGROUND igniteXmx=$Xmx
