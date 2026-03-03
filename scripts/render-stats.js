const { readFileSync, writeFileSync } = require('node:fs')

const inputPath = process.argv[2] ?? 'stats.csv'
const outputPath = process.argv[3] ?? 'stats.html'

const raw = readFileSync(inputPath, 'utf8').trim()
if (!raw) {
  throw new Error(`No data found in ${inputPath}`)
}

const lines = raw.split('\n')
const header = lines.shift()?.split(',') ?? []
const rows = lines.map((line) => line.split(','))

const indexByName = Object.fromEntries(header.map((name, index) => [name, index]))
const getValue = (row, name) => row[indexByName[name]]

function parsePercent(value) {
  return Number(value.replace('%', '')) || 0
}

function parseSizeToMiB(value) {
  const trimmed = value.trim()
  if (!trimmed) {
    return 0
  }

  const match = trimmed.match(/([0-9.]+)\s*([a-zA-Z]+)/)
  if (!match) {
    return 0
  }

  const amount = Number(match[1])
  const unit = match[2].toLowerCase()

  if (!Number.isFinite(amount)) {
    return 0
  }

  if (unit.startsWith('gib')) {
    return amount * 1024
  }

  if (unit.startsWith('mib')) {
    return amount
  }

  if (unit.startsWith('kib')) {
    return amount / 1024
  }

  if (unit.startsWith('b')) {
    return amount / (1024 * 1024)
  }

  return amount
}

const series = new Map()

for (const row of rows) {
  const timestamp = getValue(row, 'timestamp')
  const container = getValue(row, 'container')
  const cpuPercent = parsePercent(getValue(row, 'cpu_percent') ?? '0')
  const memUsage = getValue(row, 'mem_usage') ?? ''
  const memUsed = parseSizeToMiB(memUsage.split('/')[0] ?? '')

  if (!series.has(container)) {
    series.set(container, { timestamps: [], cpu: [], mem: [] })
  }

  const target = series.get(container)
  target.timestamps.push(timestamp)
  target.cpu.push(cpuPercent)
  target.mem.push(memUsed)
}

const datasetsCpu = []
const datasetsMem = []
for (const [container, values] of series.entries()) {
  datasetsCpu.push({
    label: container,
    data: values.cpu,
    tension: 0.15,
  })
  datasetsMem.push({
    label: container,
    data: values.mem,
    tension: 0.15,
  })
}

const timestamps = series.values().next().value?.timestamps ?? []

const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Kafka Benchmark Stats</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; }
      .chart { max-width: 1100px; margin-bottom: 48px; }
    </style>
  </head>
  <body>
    <h1>Kafka Benchmark Stats</h1>
    <div class="chart">
      <canvas id="cpuChart"></canvas>
    </div>
    <div class="chart">
      <canvas id="memChart"></canvas>
    </div>
    <script>
      const timestamps = ${JSON.stringify(timestamps)}
      const cpuDatasets = ${JSON.stringify(datasetsCpu)}
      const memDatasets = ${JSON.stringify(datasetsMem)}

      new Chart(document.getElementById('cpuChart'), {
        type: 'line',
        data: {
          labels: timestamps,
          datasets: cpuDatasets,
        },
        options: {
          plugins: { title: { display: true, text: 'CPU %' } },
          responsive: true,
          interaction: { mode: 'index', intersect: false },
          scales: { y: { title: { display: true, text: 'CPU %' } } },
        },
      })

      new Chart(document.getElementById('memChart'), {
        type: 'line',
        data: {
          labels: timestamps,
          datasets: memDatasets,
        },
        options: {
          plugins: { title: { display: true, text: 'Memory (MiB)' } },
          responsive: true,
          interaction: { mode: 'index', intersect: false },
          scales: { y: { title: { display: true, text: 'MiB' } } },
        },
      })
    </script>
  </body>
</html>
`

writeFileSync(outputPath, html)
console.log(`Wrote ${outputPath}`)
