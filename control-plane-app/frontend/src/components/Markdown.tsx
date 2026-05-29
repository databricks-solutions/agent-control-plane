import React from 'react'

/**
 * Minimal markdown renderer for Genie chat responses.
 *
 * Supports the subset Genie actually emits:
 *  - **bold**, *italic* / _italic_
 *  - `inline code`
 *  - [text](url) links
 *  - Bulleted lists ("- " or "* " prefix)
 *  - Ordered lists ("1. " prefix)
 *  - Blank-line paragraph breaks
 *
 * Returns React elements directly — no dangerouslySetInnerHTML, no
 * third-party dep. URLs in links are sanitized to http/https only.
 */
export default function Markdown({ text, className = '' }: { text: string; className?: string }) {
  const blocks = parseBlocks(text)
  return (
    <div className={`text-sm leading-relaxed ${className}`}>
      {blocks.map((block, i) => renderBlock(block, i))}
    </div>
  )
}

type Block =
  | { kind: 'p'; lines: string[] }
  | { kind: 'ul'; items: string[] }
  | { kind: 'ol'; items: string[] }
  | { kind: 'table'; headers: string[]; align: Array<'left' | 'right' | 'center'>; rows: string[][] }

const SEPARATOR_RE = /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)+\|?\s*$/
const TABLE_LINE_RE = /^\s*\|/

function splitCells(line: string): string[] {
  return line
    .trim()
    .replace(/^\|/, '')
    .replace(/\|$/, '')
    .split('|')
    .map(c => c.trim())
}

function alignFromSep(sep: string): Array<'left' | 'right' | 'center'> {
  return splitCells(sep).map(c => {
    const l = c.startsWith(':')
    const r = c.endsWith(':')
    if (l && r) return 'center'
    if (r) return 'right'
    return 'left'
  })
}

function parseBlocks(text: string): Block[] {
  const lines = text.split('\n')
  const blocks: Block[] = []
  let cur: Block | null = null
  const flush = () => {
    if (cur) blocks.push(cur)
    cur = null
  }

  let i = 0
  while (i < lines.length) {
    const raw = lines[i]
    const line = raw.replace(/\s+$/, '')

    if (!line.trim()) { flush(); i++; continue }

    // Table: header line followed by separator line.
    const next = lines[i + 1]?.replace(/\s+$/, '')
    if (TABLE_LINE_RE.test(line) && next && SEPARATOR_RE.test(next)) {
      flush()
      const headers = splitCells(line)
      const align = alignFromSep(next)
      const rows: string[][] = []
      i += 2
      while (i < lines.length && TABLE_LINE_RE.test(lines[i])) {
        rows.push(splitCells(lines[i]))
        i++
      }
      blocks.push({ kind: 'table', headers, align, rows })
      continue
    }

    const ul = line.match(/^\s*[-*]\s+(.*)$/)
    const ol = line.match(/^\s*\d+\.\s+(.*)$/)
    if (ul) {
      if (!cur || cur.kind !== 'ul') { flush(); cur = { kind: 'ul', items: [] } }
      cur.items.push(ul[1])
    } else if (ol) {
      if (!cur || cur.kind !== 'ol') { flush(); cur = { kind: 'ol', items: [] } }
      cur.items.push(ol[1])
    } else {
      if (!cur || cur.kind !== 'p') { flush(); cur = { kind: 'p', lines: [] } }
      cur.lines.push(line)
    }
    i++
  }
  flush()
  return blocks
}

function renderBlock(b: Block, key: number) {
  if (b.kind === 'p') {
    return (
      <p key={key} className="mb-2 last:mb-0">
        {b.lines.map((l, i) => (
          <React.Fragment key={i}>
            {i > 0 && <br />}
            {renderInline(l)}
          </React.Fragment>
        ))}
      </p>
    )
  }
  if (b.kind === 'ul') {
    return (
      <ul key={key} className="list-disc pl-5 mb-2 last:mb-0 space-y-0.5">
        {b.items.map((it, i) => <li key={i}>{renderInline(it)}</li>)}
      </ul>
    )
  }
  if (b.kind === 'ol') {
    return (
      <ol key={key} className="list-decimal pl-5 mb-2 last:mb-0 space-y-0.5">
        {b.items.map((it, i) => <li key={i}>{renderInline(it)}</li>)}
      </ol>
    )
  }
  // table
  const alignClass = (i: number) => {
    const a = b.align[i] ?? 'left'
    return a === 'right' ? 'text-right' : a === 'center' ? 'text-center' : 'text-left'
  }
  return (
    <div key={key} className="mb-2 last:mb-0 overflow-x-auto rounded-md border border-gray-200 dark:border-gray-700">
      <table className="w-full text-xs border-collapse">
        <thead>
          <tr className="bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
            {b.headers.map((h, i) => (
              <th
                key={i}
                className={`px-2 py-1.5 font-medium text-gray-600 dark:text-gray-300 whitespace-nowrap ${alignClass(i)}`}
              >
                {renderInline(h)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {b.rows.map((row, ri) => (
            <tr key={ri} className="border-b border-gray-100 dark:border-gray-800 last:border-b-0">
              {row.map((cell, ci) => (
                <td
                  key={ci}
                  className={`px-2 py-1 text-db-navy-900 dark:text-gray-200 ${alignClass(ci)} ${
                    /^[\d,.\s$%-]+$/.test(cell) ? 'font-mono tabular-nums' : ''
                  }`}
                >
                  {renderInline(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/** Inline tokenizer — handles **bold**, *italic*, `code`, [text](url). */
function renderInline(text: string): React.ReactNode {
  const tokens: React.ReactNode[] = []
  // Single pass over the string, consuming the first matching pattern at each position.
  const patterns: Array<{ re: RegExp; render: (m: RegExpExecArray) => React.ReactNode }> = [
    {
      // Bold first (longer marker), to avoid getting captured by italic
      re: /\*\*([^*]+)\*\*/y,
      render: m => <strong className="font-semibold">{m[1]}</strong>,
    },
    {
      re: /\*([^*\n]+)\*/y,
      render: m => <em>{m[1]}</em>,
    },
    {
      re: /_([^_\n]+)_/y,
      render: m => <em>{m[1]}</em>,
    },
    {
      re: /`([^`\n]+)`/y,
      render: m => <code className="px-1 py-0.5 rounded bg-gray-100 dark:bg-gray-900 text-[0.85em]">{m[1]}</code>,
    },
    {
      re: /\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/y,
      render: m => (
        <a href={m[2]} target="_blank" rel="noopener noreferrer" className="text-db-red hover:underline">
          {m[1]}
        </a>
      ),
    },
  ]
  let i = 0
  let plain = ''
  const pushPlain = () => {
    if (plain) {
      tokens.push(plain)
      plain = ''
    }
  }
  while (i < text.length) {
    let matched = false
    for (const p of patterns) {
      p.re.lastIndex = i
      const m = p.re.exec(text)
      if (m && m.index === i) {
        pushPlain()
        tokens.push(<React.Fragment key={tokens.length}>{p.render(m)}</React.Fragment>)
        i += m[0].length
        matched = true
        break
      }
    }
    if (!matched) {
      plain += text[i]
      i++
    }
  }
  pushPlain()
  return tokens
}
