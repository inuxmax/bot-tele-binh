const maleNames = [
  'An', 'Bình', 'Cường', 'Dũng', 'Hùng', 'Huy', 'Khánh', 'Kiên', 'Long', 'Nam',
  'Phong', 'Quân', 'Sơn', 'Thành', 'Thắng', 'Tuấn', 'Việt', 'Vinh', 'Quang', 'Tài',
  'Hòa', 'Mạnh', 'Phúc', 'Thái', 'Toàn', 'Tùng', 'Vũ', 'Hiếu', 'Hoàng', 'Thiện',
  'Trường', 'Anh', 'Duy', 'Hải', 'Tiến', 'Đạt', 'Khoa', 'Bảo', 'Đăng', 'Khôi',
  'Nguyên', 'Phát', 'Sang', 'Đinh', 'Luân', 'Tú', 'Chung', 'Quốc', 'Vỹ', 'Hải', 'Nhật'
];

const femaleNames = [
  'Ánh', 'Dung', 'Hạnh', 'Hương', 'Lan', 'Loan', 'Mai', 'Ngọc', 'Phương',
  'Thu', 'Trang', 'Vân', 'Yến', 'Hà', 'Hằng', 'Hoa', 'Hồng', 'Huyền', 'Kim', 'Minh',
  'Nga', 'Nhung', 'Quỳnh', 'Thanh', 'Thảo', 'Diệu', 'Linh', 'My', 'Thủy', 'Vy',
  'Trâm', 'Uyên', 'Nhi', 'Châu', 'Ly', 'Trinh', 'Thư', 'Như', 'Bích', 'Trúc', 'Chi'
];

function randomName() {
  // Cũ: fallback (nếu vẫn có chỗ dùng)
  const prefix = ['Nguyễn Văn', 'Trần Văn', 'Lê Văn', 'Phạm Văn', 'Nguyễn Thị', 'Trần Thị', 'Lê Thị', 'Phạm Thị'];
  const p = prefix[Math.floor(Math.random() * prefix.length)];
  if (p.includes('Thị')) {
    return `${p} ${femaleNames[Math.floor(Math.random() * femaleNames.length)]}`;
  } else {
    return `${p} ${maleNames[Math.floor(Math.random() * maleNames.length)]}`;
  }
}

function randomFirstName(gender) {
  if (gender === 'Nam') {
    return maleNames[Math.floor(Math.random() * maleNames.length)];
  } else {
    return femaleNames[Math.floor(Math.random() * femaleNames.length)];
  }
}

module.exports = { randomName, randomFirstName };
