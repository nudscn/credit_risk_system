import Foundation
import PDFKit

func emit(_ obj: Any) {
    guard JSONSerialization.isValidJSONObject(obj),
          let data = try? JSONSerialization.data(withJSONObject: obj, options: []) else {
        fputs("{\"engine\":null,\"status\":\"failed\",\"pages\":[],\"error\":\"JSON serialization failed\"}\n", stderr)
        exit(2)
    }
    if let s = String(data: data, encoding: .utf8) {
        print(s)
    }
}

guard CommandLine.arguments.count >= 2 else {
    emit([
        "engine": NSNull(),
        "status": "failed",
        "pages": [],
        "error": "Missing PDF path argument"
    ])
    exit(1)
}

let pdfPath = CommandLine.arguments[1]
let url = URL(fileURLWithPath: pdfPath)

guard let doc = PDFDocument(url: url) else {
    emit([
        "engine": NSNull(),
        "status": "failed",
        "pages": [],
        "error": "Unable to open PDF via PDFKit"
    ])
    exit(1)
}

var pages: [[String: Any]] = []
for idx in 0..<doc.pageCount {
    let pageNo = idx + 1
    let text = doc.page(at: idx)?.string ?? ""
    pages.append([
        "page": pageNo,
        "text": text,
        "chars": text.count
    ])
}

emit([
    "engine": "swift_pdfkit",
    "status": "ok",
    "pages": pages
])
