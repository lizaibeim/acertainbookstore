package com.acertainbookstore.business;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;
import com.acertainbookstore.utils.BookStoreUtility;

/** {@link TwoLevelLockingConcurrentCertainBookStore} implements the {@link BookStore} and
 * {@link StockManager} functionalities.
 * 
 * @see BookStore
 * @see StockManager
 */
public class TwoLevelLockingConcurrentCertainBookStore implements BookStore, StockManager {

	/** The mapping of books from ISBN to {@link BookStoreBook}. */
	private Map<Integer, BookStoreBook> bookMap = null;
	private ConcurrentHashMap<Integer, ReentrantReadWriteLock> lockMap; // the mapping of locks from ISBN to lock
	private ReentrantReadWriteLock lock; //Read Write lock for db
	private IntentionalLock intentionalLock; //Intentional lock for db

	/**
	 * Instantiates a new {@link CertainBookStore}.
	 */
	public TwoLevelLockingConcurrentCertainBookStore() {
		// Constructors are not synchronized
		bookMap = new HashMap<>();
		lockMap = new ConcurrentHashMap<>();
		lock = new ReentrantReadWriteLock();
		intentionalLock = new IntentionalLock(lock);
	}
	
	private void validate(StockBook book) throws BookStoreException {
		int isbn = book.getISBN();
		String bookTitle = book.getTitle();
		String bookAuthor = book.getAuthor();
		int noCopies = book.getNumCopies();
		float bookPrice = book.getPrice();

		if (BookStoreUtility.isInvalidISBN(isbn)) { // Check if the book has valid ISBN
			throw new BookStoreException(BookStoreConstants.ISBN + isbn + BookStoreConstants.INVALID);
		}

		if (BookStoreUtility.isEmpty(bookTitle)) { // Check if the book has valid title
			throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
		}

		if (BookStoreUtility.isEmpty(bookAuthor)) { // Check if the book has valid author
			throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
		}

		if (BookStoreUtility.isInvalidNoCopies(noCopies)) { // Check if the book has at least one copy
			throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
		}

		if (bookPrice < 0.0) { // Check if the price of the book is valid
			throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
		}

		if (bookMap.containsKey(isbn)) {// Check if the book is not in stock
			throw new BookStoreException(BookStoreConstants.ISBN + isbn + BookStoreConstants.DUPLICATED);
		}
	}	
	
	private void validate(BookCopy bookCopy) throws BookStoreException {
		int isbn = bookCopy.getISBN();
		int numCopies = bookCopy.getNumCopies();

		validateISBNInStock(isbn); // Check if the book has valid ISBN and in stock

		if (BookStoreUtility.isInvalidNoCopies(numCopies)) { // Check if the number of the book copy is larger than zero
			throw new BookStoreException(BookStoreConstants.NUM_COPIES + numCopies + BookStoreConstants.INVALID);
		}
	}
	
	private void validate(BookEditorPick editorPickArg) throws BookStoreException {
		int isbn = editorPickArg.getISBN();
		validateISBNInStock(isbn); // Check if the book has valid ISBN and in stock
	}
	
	private void validateISBNInStock(Integer ISBN) throws BookStoreException {
		if (BookStoreUtility.isInvalidISBN(ISBN)) { // Check if the book has valid ISBN
			throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.INVALID);
		}

		if (!bookMap.containsKey(ISBN)) {// Check if the book is in stock
			throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.NOT_AVAILABLE);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.acertainbookstore.interfaces.StockManager#addBooks(java.util.Set)
	 */
	public void addBooks(Set<StockBook> bookSet) throws BookStoreException {
		if (bookSet == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}

		// acquire the exclusive lock of database when add books
		lock.writeLock().lock();

		try {
			// Check if all are there
			for (StockBook book : bookSet) {
				validate(book);
			}

			for (StockBook book : bookSet) {
				int isbn = book.getISBN();
				bookMap.put(isbn, new BookStoreBook(book));
				lockMap.put(isbn, new ReentrantReadWriteLock());
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.acertainbookstore.interfaces.StockManager#addCopies(java.util.Set)
	 */
	public void addCopies(Set<BookCopy> bookCopiesSet) throws BookStoreException {
		int isbn;
		int numCopies;

		if (bookCopiesSet == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}

		// acquire the intentional exclusive lock on db
		intentionalLock.AcquireIntentionExclusiveLock();
		HashSet<BookCopy> validatedCopies = new HashSet<>();


		for (BookCopy bookCopy : bookCopiesSet) {

			try {
				validate(bookCopy);
				lockMap.get(bookCopy.getISBN()).writeLock().lock();
				validatedCopies.add(bookCopy);
			}
			catch (BookStoreException e) {
				releaseAllLocks();
				///for(BookCopy validatedBookCopy : validatedCopies) {
				///	lockMap.get(validatedBookCopy.getISBN()).writeLock().unlock();
				///}
				///intentionalLock.ReleaseIntentionExclusiveLock();
				throw new BookStoreException(e);
			}
		}

		BookStoreBook book;

		// Update the number of copies
		for (BookCopy bookCopy : bookCopiesSet) {
			isbn = bookCopy.getISBN();
			numCopies = bookCopy.getNumCopies();

			book = bookMap.get(isbn);
			book.addCopies(numCopies);
			//lockMap.get(isbn).writeLock().unlock();
		}

		//intentionalLock.ReleaseIntentionExclusiveLock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.acertainbookstore.interfaces.StockManager#getBooks()
	 */
	public List<StockBook> getBooks() throws BookStoreException{
		// acquire intentional share lock
		intentionalLock.AcquireIntentionShareLock();
		for (HashMap.Entry book : bookMap.entrySet()){
			lockMap.get(book.getKey()).readLock().lock();
		}


		Collection<BookStoreBook> bookMapValues = bookMap.values();


		for (HashMap.Entry book : bookMap.entrySet()){
			lockMap.get(book.getKey()).readLock().unlock();
		}
		intentionalLock.ReleaseIntentionShareLock();

		//
		return bookMapValues.stream()
				.map(book -> book.immutableStockBook())
				.collect(Collectors.toList());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.acertainbookstore.interfaces.StockManager#updateEditorPicks(java.util
	 * .Set)
	 */
	public void updateEditorPicks(Set<BookEditorPick> editorPicks) throws BookStoreException {
		// Check that all ISBNs that we add/remove are there first.
		if (editorPicks == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}

		int isbnValue;

		// acquire the intentional exclusive lock
		intentionalLock.AcquireIntentionExclusiveLock();
		HashSet<BookEditorPick> validatedPicks = new HashSet<>();

		for (BookEditorPick editorPickArg : editorPicks) {
			try {
				validate(editorPickArg);
				lockMap.get(editorPickArg.getISBN()).writeLock().lock();
				validatedPicks.add(editorPickArg);

			}
			catch (BookStoreException e){
				for (BookEditorPick validatedPickArg : validatedPicks)
				{
					lockMap.get(validatedPickArg.getISBN()).writeLock().unlock();
				}
				intentionalLock.ReleaseIntentionExclusiveLock();
			}
		}

		for (BookEditorPick editorPickArg : editorPicks) {
			bookMap.get(editorPickArg.getISBN()).setEditorPick(editorPickArg.isEditorPick());
			lockMap.get(editorPickArg.getISBN()).writeLock().unlock();
		}

		// release the intentional exclusive lock
		intentionalLock.ReleaseIntentionExclusiveLock();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.acertainbookstore.interfaces.BookStore#buyBooks(java.util.Set)
	 */
	public void buyBooks(Set<BookCopy> bookCopiesToBuy) throws BookStoreException {
		if (bookCopiesToBuy == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}

		// Check that all ISBNs that we buy are there first.
		int isbn;
		BookStoreBook book;
		Boolean saleMiss = false;

		Map<Integer, Integer> salesMisses = new HashMap<>();

		// acquire intentional exclusive lock
		intentionalLock.AcquireIntentionExclusiveLock();
		HashSet<BookCopy> validatedCopies = new HashSet<>();

		for (BookCopy bookCopyToBuy : bookCopiesToBuy) {
			isbn = bookCopyToBuy.getISBN();
			try{
				validate(bookCopyToBuy);
				lockMap.get(bookCopyToBuy.getISBN()).writeLock().lock();
				validatedCopies.add(bookCopyToBuy);

			}
			catch (BookStoreException e) {
				releaseAllLocks();
//				for (BookCopy validatedCopy : validatedCopies){
//					lockMap.get(validatedCopy.getISBN()).writeLock().unlock();
//				}
//				intentionalLock.ReleaseIntentionExclusiveLock();
				throw new BookStoreException(e);
			}

			book = bookMap.get(isbn);

			if (!book.areCopiesInStore(bookCopyToBuy.getNumCopies())) {
				// If we cannot sell the copies of the book, it is a miss.
				salesMisses.put(isbn, bookCopyToBuy.getNumCopies() - book.getNumCopies());
				saleMiss = true;
			}
		}

		// We throw exception now since we want to see how many books in the
		// order incurred misses which is used by books in demand
		if (saleMiss) {
			for (Map.Entry<Integer, Integer> saleMissEntry : salesMisses.entrySet()) {
				book = bookMap.get(saleMissEntry.getKey());
				book.addSaleMiss(saleMissEntry.getValue());
			}

			releaseAllLocks();
//			for (BookCopy bookCopyToBuy : bookCopiesToBuy) {
//				lockMap.get(bookCopyToBuy.getISBN()).writeLock().unlock();
//			}
//			intentionalLock.ReleaseIntentionExclusiveLock();
			throw new BookStoreException(BookStoreConstants.BOOK + BookStoreConstants.NOT_AVAILABLE);
		}

		// Then make the purchase.
		for (BookCopy bookCopyToBuy : bookCopiesToBuy) {
			book = bookMap.get(bookCopyToBuy.getISBN());
			book.buyCopies(bookCopyToBuy.getNumCopies());
			//lockMap.get(bookCopyToBuy.getISBN()).writeLock().unlock();
		}
		//intentionalLock.ReleaseIntentionExclusiveLock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.acertainbookstore.interfaces.StockManager#getBooksByISBN(java.util.
	 * Set)
	 */
	public List<StockBook> getBooksByISBN(Set<Integer> isbnSet) throws BookStoreException {
		if (isbnSet == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}

		// acquire intentional share lock
		intentionalLock.AcquireIntentionShareLock();
		HashSet<Integer> validatedISBN = new HashSet<>();

		for (Integer ISBN : isbnSet) {
			try {
				validateISBNInStock(ISBN);
				lockMap.get(ISBN).readLock().lock();
				validatedISBN.add(ISBN);

			}
			catch (BookStoreException e) {
				for (Integer validatedIsbn : validatedISBN){
					lockMap.get(validatedIsbn).readLock().unlock();
				}
				intentionalLock.ReleaseIntentionShareLock();
				throw new BookStoreException(e);
			}
		}


		List<StockBook> books =  isbnSet.stream()
				.map(isbn -> bookMap.get(isbn).immutableStockBook())
				.collect(Collectors.toList());

		// release locks
		for (Integer ISBN : isbnSet) {
			lockMap.get(ISBN).readLock().unlock();
		}
		intentionalLock.ReleaseIntentionShareLock();

		return books;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.acertainbookstore.interfaces.BookStore#getBooks(java.util.Set)
	 */
	public List<Book> getBooks(Set<Integer> isbnSet) throws BookStoreException {
		if (isbnSet == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}

		intentionalLock.AcquireIntentionShareLock();
		HashSet<Integer> validatedISBN = new HashSet<>();

		// Check that all ISBNs that we rate are there to start with.
		for (Integer ISBN : isbnSet) {
			try	{
				validateISBNInStock(ISBN);
				lockMap.get(ISBN).readLock().lock();
				validatedISBN.add(ISBN);
			} catch (BookStoreException e) {
				for (Integer validatedIsbn : validatedISBN) {
					lockMap.get(validatedIsbn).readLock().unlock();
				}
				intentionalLock.ReleaseIntentionShareLock();
				throw new BookStoreException(e);
			}
		}

		List<Book> books = isbnSet.stream()
				.map(isbn -> bookMap.get(isbn).immutableBook())
				.collect(Collectors.toList());

		for (Integer ISBN : isbnSet) {
			lockMap.get(ISBN).readLock().unlock();
		}
		intentionalLock.ReleaseIntentionShareLock();

		return books;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.acertainbookstore.interfaces.BookStore#getEditorPicks(int)
	 */
	public List<Book> getEditorPicks(int numBooks) throws BookStoreException {
		if (numBooks < 0) {
			throw new BookStoreException("numBooks = " + numBooks + ", but it must be positive");
		}

		intentionalLock.AcquireIntentionShareLock();
		for (HashMap.Entry book : bookMap.entrySet()) {
			lockMap.get(book.getKey()).readLock().lock();
		}

		List<BookStoreBook> listAllEditorPicks = bookMap.entrySet().stream()
				.map(pair -> pair.getValue())
				.filter(book -> book.isEditorPick())
				.collect(Collectors.toList());

		for (HashMap.Entry book : bookMap.entrySet()) {
			lockMap.get(book.getKey()).readLock().unlock();
		}
		intentionalLock.ReleaseIntentionShareLock();

		// Find numBooks random indices of books that will be picked.
		Random rand = new Random();
		Set<Integer> tobePicked = new HashSet<>();
		int rangePicks = listAllEditorPicks.size();

		if (rangePicks <= numBooks) {

			// We need to add all books.
			for (int i = 0; i < listAllEditorPicks.size(); i++) {
				tobePicked.add(i);
			}
		} else {

			// We need to pick randomly the books that need to be returned.
			int randNum;

			while (tobePicked.size() < numBooks) {
				randNum = rand.nextInt(rangePicks);
				tobePicked.add(randNum);
			}
		}

		// Return all the books by the randomly chosen indices.
		return tobePicked.stream()
				.map(index -> listAllEditorPicks.get(index).immutableBook())
				.collect(Collectors.toList());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.acertainbookstore.interfaces.BookStore#getTopRatedBooks(int)
	 */
	@Override
	public List<Book> getTopRatedBooks(int numBooks) throws BookStoreException {
		throw new BookStoreException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.acertainbookstore.interfaces.StockManager#getBooksInDemand()
	 */
	@Override
	public List<StockBook> getBooksInDemand() throws BookStoreException {
		throw new BookStoreException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.acertainbookstore.interfaces.BookStore#rateBooks(java.util.Set)
	 */
	@Override
	public void rateBooks(Set<BookRating> bookRating) throws BookStoreException {
		throw new BookStoreException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.acertainbookstore.interfaces.StockManager#removeAllBooks()
	 */
	public void removeAllBooks() throws BookStoreException {
		intentionalLock.AcquireIntentionExclusiveLock();
		for (HashMap.Entry book : bookMap.entrySet()) {
			lockMap.get(book.getKey()).writeLock().lock();
		}

		bookMap.clear();

		for (HashMap.Entry book : bookMap.entrySet()) {
			lockMap.get(book.getKey()).writeLock().unlock();
		}
		intentionalLock.ReleaseIntentionExclusiveLock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.acertainbookstore.interfaces.StockManager#removeBooks(java.util.Set)
	 */
	public void removeBooks(Set<Integer> isbnSet) throws BookStoreException {
		if (isbnSet == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}

		intentionalLock.AcquireIntentionExclusiveLock();
		HashSet<Integer> validatedISBN = new HashSet<>();

		for (Integer ISBN : isbnSet) {
			if (BookStoreUtility.isInvalidISBN(ISBN)) {
				for (Integer validatedIsbn : validatedISBN) {
					lockMap.get(validatedIsbn).writeLock().unlock();
				}
				intentionalLock.ReleaseIntentionExclusiveLock();
				throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.INVALID);
			}

			if (!bookMap.containsKey(ISBN)) {
				for (Integer validatedIsbn : validatedISBN) {
					lockMap.get(validatedIsbn).writeLock().unlock();
				}
				intentionalLock.ReleaseIntentionExclusiveLock();
				throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.NOT_AVAILABLE);
			}

			lockMap.get(ISBN).writeLock().lock();
			validatedISBN.add(ISBN);
		}

		for (int isbn : isbnSet) {
			bookMap.remove(isbn);
			lockMap.get(isbn).writeLock().unlock();
		}

		intentionalLock.ReleaseIntentionExclusiveLock();
	}

	public void releaseAllLocks() throws BookStoreException
	{
		// release all read / write lock
		int readHoldCount;
		int writeHoldCount;

		for (HashMap.Entry book : bookMap.entrySet())
		{
		 	readHoldCount = lockMap.get(book.getKey()).getReadHoldCount();
		 	writeHoldCount = lockMap.get(book.getKey()).getWriteHoldCount();
			for (int i = 0; i < readHoldCount; i++){
				lockMap.get(book.getKey()).readLock().unlock();
			}
			for (int j = 0; j < writeHoldCount; j++){
				lockMap.get(book.getKey()).writeLock().unlock();
			}
		}

		// release all intentional lock
		int intentionReadHoldCount = intentionalLock.getReadHolderCount();
		int intentionWriteHoldCount = intentionalLock.getWriteHolderCount();

		for (int i = 0; i < intentionReadHoldCount; i++){
			intentionalLock.ReleaseIntentionShareLock();
		}

		for (int j = 0; j < intentionWriteHoldCount; j++) {
			intentionalLock.ReleaseIntentionExclusiveLock();
		}
	}
}

class IntentionalLock {
	// Read Write lock
	private ReentrantReadWriteLock lock;

	// two intentional locks mapping from Thread ID to Reentrant count
	private ConcurrentHashMap<Long, Integer> readHolder = null;
	private ConcurrentHashMap<Long, Integer> writeHolder = null;

	// constructor
	public IntentionalLock( ReentrantReadWriteLock readWriteLock) {
		readHolder = new ConcurrentHashMap<>();
		writeHolder = new ConcurrentHashMap<>();
		lock = readWriteLock;
	}

	// get read holder count for specific thread
	public int getReadHolderCount(){
		Long ID = Thread.currentThread().getId();
		if (readHolder.containsKey(ID))
			return readHolder.get(ID).intValue();
		else return 0;
	}

	// get write holder count for specific thread
	public int getWriteHolderCount(){
		Long ID = Thread.currentThread().getId();
		if (writeHolder.containsKey(ID))
			return writeHolder.get(ID).intValue();
		else return 0;
	}

	// check whether the current thread has intentional share lock
	public boolean IsIntentionReadLockedByOthers()
	{
		Long ID = Thread.currentThread().getId();
		int itself = readHolder.containsKey(ID) ? 1 : 0;
		boolean isIntentionShareLock = (readHolder.size() - itself > 0) ? true : false;
		return  isIntentionShareLock;
	}

	// check whether the other thread has intentional exclusive lock
	public boolean IsIntentionWriteLockedByOthers() {
		Long ID = Thread.currentThread().getId();
		int itself = writeHolder.containsKey(ID) ? 1 : 0;
		boolean isIntentionExclusiveLock = (writeHolder.size() - itself > 0) ? true : false;
		return isIntentionExclusiveLock;
	}

	// acquire intentional share lock
	public synchronized void AcquireIntentionShareLock() {
//		If the exclusive lock of the entire database is already occupied, then wait.
//		while (!lock.isWriteLockedByCurrentThread() && lock.isWriteLocked()){
//			wait();
//		}

		Long ID = Thread.currentThread().getId();
		if (readHolder.containsKey(ID)) {
			int newVal =  readHolder.get(ID) + 1;
			readHolder.replace(ID, newVal);
		}
		else {
			readHolder.put(ID, 1);
		}
	}

	// release intentional share lock
	public void ReleaseIntentionShareLock() throws BookStoreException {
		Long ID = Thread.currentThread().getId();
		if (readHolder.containsKey(ID)) {
			int newVal = readHolder.get(ID) - 1;
			readHolder.replace(ID, newVal);
			if (newVal == 0)
				readHolder.remove(ID);
		}
		else {
			throw new BookStoreException("Should acquire intentional share lock before release");
		}
	}

	// acquire intentional exclusive lock
	public synchronized void AcquireIntentionExclusiveLock() {
//		if the share or exclusive lock of the entire database is already occupied, then wait.
//		while (lock.getReadLockCount() != lock.getReadHoldCount() || (!lock.isWriteLockedByCurrentThread() && lock.isWriteLocked())){
//			wait();
//		}

		Long ID = Thread.currentThread().getId();
		if (writeHolder.containsKey(ID)) {
			int newVal = writeHolder.get(ID) + 1;
			writeHolder.replace(ID, newVal);
		}
		else {
			writeHolder.put(ID, 1);
		}
	}

	// release intentional exclusive lock
	public void ReleaseIntentionExclusiveLock() throws BookStoreException {
		Long ID = Thread.currentThread().getId();
		if (writeHolder.containsKey(ID)) {
			int newVal	= writeHolder.get(ID) - 1;
			writeHolder.replace(ID, newVal);
			if (newVal == 0)
				writeHolder.remove(ID);
		}
		else {
			throw new BookStoreException("Should acquire intentional exclusive lock before release");
		}

	}

}
