import Foundation
import PDFKit
import Vision
import AppKit

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
let maxPages = CommandLine.arguments.count >= 3 ? Int(CommandLine.arguments[2]) ?? 20 : 20

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

let targetPages = min(doc.pageCount, maxPages)
var pages: [[String: Any]] = []

for idx in 0..<targetPages {
    guard let page = doc.page(at: idx) else { continue }
    let size = NSSize(width: 2200, height: 3000)
    let image = page.thumbnail(of: size, for: .mediaBox)
    guard let cgImage = image.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
        continue
    }

    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = false

    let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
    do {
        try handler.perform([request])
        let results = request.results ?? []
        let lines = results.compactMap { observation -> String? in
            observation.topCandidates(1).first?.string
        }
        let text = lines.joined(separator: "\n")
        pages.append([
            "page": idx + 1,
            "text": text,
            "chars": text.count
        ])
    } catch {
        continue
    }
}

emit([
    "engine": "swift_vision_ocr",
    "status": "ok",
    "processed_pages": targetPages,
    "pages": pages
])
