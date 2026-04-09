import { ChevronLeft, ChevronRight } from 'lucide-react'

const ROW_OPTIONS = [10, 25, 50]

interface TablePaginationProps {
  /** 0-based current page index */
  page: number
  /** Total number of items (unsliced) */
  totalItems: number
  /** Rows shown per page */
  pageSize: number
  onPageChange: (page: number) => void
  onPageSizeChange: (size: number) => void
}

export function TablePagination({
  page,
  totalItems,
  pageSize,
  onPageChange,
  onPageSizeChange,
}: TablePaginationProps) {
  if (totalItems === 0) return null

  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize))
  const safePage = Math.min(page, totalPages - 1)
  const rangeStart = safePage * pageSize + 1
  const rangeEnd = Math.min((safePage + 1) * pageSize, totalItems)

  return (
    <div className="flex items-center justify-end gap-6 pt-4 text-sm text-gray-600 dark:text-gray-400">
      {/* Rows per page */}
      <div className="flex items-center gap-2">
        <span className="text-gray-500 dark:text-gray-400 whitespace-nowrap">Rows per page:</span>
        <select
          value={pageSize}
          onChange={(e) => {
            onPageSizeChange(Number(e.target.value))
            onPageChange(0)
          }}
          className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1 text-sm bg-white dark:bg-gray-700 dark:text-gray-200 focus:ring-1 focus:ring-gray-400 focus:border-gray-400"
        >
          {ROW_OPTIONS.map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
        </select>
      </div>

      {/* Range indicator */}
      <span className="tabular-nums whitespace-nowrap">
        {rangeStart} – {rangeEnd} of {totalItems}
      </span>

      {/* Prev / Next */}
      <div className="flex items-center gap-1">
        <button
          onClick={() => onPageChange(safePage - 1)}
          disabled={safePage === 0}
          className="p-1 rounded-md text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-30 disabled:hover:bg-transparent transition-colors"
          aria-label="Previous page"
        >
          <ChevronLeft className="w-5 h-5" />
        </button>
        <button
          onClick={() => onPageChange(safePage + 1)}
          disabled={safePage >= totalPages - 1}
          className="p-1 rounded-md text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-30 disabled:hover:bg-transparent transition-colors"
          aria-label="Next page"
        >
          <ChevronRight className="w-5 h-5" />
        </button>
      </div>
    </div>
  )
}
