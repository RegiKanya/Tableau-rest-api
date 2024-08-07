from reportlab.pdfgen.canvas import Canvas

pdf_file_name = '/Users/regina.kanya/Desktop/fake_CV.pdf'

canvas = Canvas("fake_CV.pdf")
canvas.drawString(72, 792, "This is a fake CV.")
canvas.save()